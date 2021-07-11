#     Copyright 2020. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import time
import subprocess
import sys
import simplejson
from random import choice
from pprint import pformat
from threading import Thread
from string import ascii_lowercase

from bluepy import __path__ as bluepy_path
from bluepy.btle import DefaultDelegate, Peripheral, Scanner, UUID, capitaliseName, BTLEInternalError
from bluepy.btle import BTLEDisconnectError, BTLEManagementError, BTLEGattError

from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.connectors.ble.bytes_ble_uplink_converter import BytesBLEUplinkConverter
from thingsboard_gateway.connectors.ble.ble_storage_helper import BLEStorageHelper
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class BLEConnector(Connector, Thread):
    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.__connector_type = connector_type
        self.__default_services = list(range(0x1800, 0x183A))
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        self.__gateway = gateway
        self.__config = config
        self.setName(self.__config.get("name",
                                       'BLE Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5))))
        self.__storage = BLEStorageHelper()

        self._connected = False
        self.__stopped = False
        self.__check_interval_seconds = self.__config['checkIntervalSeconds'] if self.__config.get(
            'checkIntervalSeconds') is not None else 10
        self.__rescan_time = self.__config['rescanIntervalSeconds'] if self.__config.get(
            'rescanIntervalSeconds') is not None else 10
        self.__disconnect_after_use = self.__config['disconnectAfterUse'] if self.__config.get(
            'disconnectAfterUse') is not None else False
        self.__disconnect_before_scan = self.__config['disconnectBeforeScan'] if self.__config.get(
            'disconnectBeforeScan') is not None else False
        self.__disconnect_safe_wait = self.__config['disconnectSafeWaitSeconds'] if self.__config.get(
            'disconnectSafeWaitSeconds') is not None else 0
        self.__reboot_if_no_data_since_seconds = self.__config['rebootIfNoDataSinceSeconds'] if self.__config.get(
            'rebootIfNoDataSinceSeconds') is not None else 0
        self.__filepath_scanned_device_list = self.__config['scannedDeviceListFilepath'] if self.__config.get(
            'scannedDeviceListFilepath') is not None else None
        self.__rssi_as_telemetry = self.__config['rssiAsTelemetry'] if self.__config.get(
            'rssiAsTelemetry') is not None else None
        self.__log_as_telemetry = self.__config['logAsTelemetry'] if self.__config.get(
            'logAsTelemetry') is not None else False
        self.__previous_scan_time = time.time() - self.__rescan_time
        self.__previous_read_time = time.time() - self.__check_interval_seconds
        self.__previous_send_data_time = time.time()
        self.__scanner = Scanner().withDelegate(ScanDelegate(self))
        self.__devices_around = {}
        self.__devices_scanned = []
        self.__available_converters = []
        self.__notify_delegators = {}
        self.__fill_interest_devices()
        self.daemon = True
        self.reconnect_try_count = 5

    def run(self):
        while True:
            if time.time() - self.__previous_scan_time >= self.__rescan_time != 0:
                self.__scan_ble()
                self.__previous_scan_time = time.time()

            if time.time() - self.__previous_read_time >= self.__check_interval_seconds:
                self.__get_services_and_chars()
                self.__previous_read_time = time.time()

            if self.__reboot_if_no_data_since_seconds > 0:
                no_data_since_seconds = time.time() - self.__previous_send_data_time
                if no_data_since_seconds >= self.__reboot_if_no_data_since_seconds:
                    self.__log_and_send_info("No data since %i seconds (timeout setpoint %i)", no_data_since_seconds, self.__reboot_if_no_data_since_seconds)
                    self.__perform_system_reboot()

            time.sleep(.1)
            if self.__stopped:
                log.debug('STOPPED')
                break

    def close(self):
        self.__stopped = True
        for device in self.__devices_around:
            try:
                if self.__devices_around[device].get('peripheral') is not None:
                    self.__log_and_send_info("Disconnecting device %s", device)
                    self.__devices_around[device]['peripheral'].disconnect()
            except Exception as e:
                log.exception(e)
                self.__log_and_send_error("Unable to close the device. Further operations may fail. Try to restart or reconnect your BLE hardware.")
                # raise e # Don't raise exception to avoid permanent blocking of the system!

    def get_name(self):
        return self.name

    def on_attributes_update(self, content):
        log.debug(content)
        for device in self.__devices_around:
            if self.__devices_around[device]['device_config'].get('name') == content['device']:
                for requests in self.__devices_around[device]['device_config']["attributeUpdates"]:
                    for service in self.__devices_around[device]['services']:
                        if requests['characteristicUUID'] in self.__devices_around[device]['services'][service]:
                            characteristic = self.__devices_around[device]['services'][service][requests['characteristicUUID']]['characteristic']
                            if 'WRITE' in characteristic.propertiesToString():
                                if content['data'].get(requests['attributeOnThingsBoard']) is not None:
                                    try:
                                        self.__check_and_reconnect(device)
                                        content_to_write = content['data'][requests['attributeOnThingsBoard']].encode('UTF-8')
                                        characteristic.write(content_to_write, True)
                                    except BTLEDisconnectError:
                                        self.__check_and_reconnect(device)
                                        content_to_write = content['data'][requests['attributeOnThingsBoard']].encode('UTF-8')
                                        characteristic.write(content_to_write, True)
                                    except Exception as e:
                                        self.__log_and_send_info("Unable to write char[%s] for device %s", requests['characteristicUUID'], device)
                                        log.exception(e)
                            else:
                                self.__log_and_send_error(
                                    'Cannot process attribute update request for device: %s with data: %s and config: %s',
                                    device,
                                    content,
                                    self.__devices_around[device]['device_config']["attributeUpdates"])

    def server_side_rpc_handler(self, content):
        log.debug(content)
        try:
            for device in self.__devices_around:
                if self.__devices_around[device]['device_config'].get('name') == content['device']:
                    for requests in self.__devices_around[device]['device_config']["serverSideRpc"]:
                        for service in self.__devices_around[device]['services']:
                            if requests['characteristicUUID'] in self.__devices_around[device]['services'][service]:
                                characteristic = self.__devices_around[device]['services'][service][requests['characteristicUUID']]['characteristic']
                                if requests.get('methodProcessing') and requests['methodProcessing'].upper() in characteristic.propertiesToString():
                                    if content['data']['method'] == requests['methodRPC']:
                                        response = None
                                        if requests['methodProcessing'].upper() == 'WRITE':
                                            try:
                                                self.__check_and_reconnect(device)
                                                response = characteristic.write(content['data'].get('params', '').encode('UTF-8'),
                                                                                requests.get('withResponse', False))
                                            except BTLEDisconnectError:
                                                self.__check_and_reconnect(device)
                                                response = characteristic.write(content['data'].get('params', '').encode('UTF-8'),
                                                                                requests.get('withResponse', False))
                                        elif requests['methodProcessing'].upper() == 'READ':
                                            try:
                                                self.__check_and_reconnect(device)
                                                response = characteristic.read()
                                            except BTLEDisconnectError:
                                                self.__check_and_reconnect(device)
                                                response = characteristic.read()
                                        elif requests['methodProcessing'].upper() == 'NOTIFY':
                                            try:
                                                self.__check_and_reconnect(device)
                                                delegate = self.__notify_handler(self.__devices_around[device],
                                                                                 characteristic.handle)
                                                response = delegate.data
                                            except BTLEDisconnectError:
                                                self.__check_and_reconnect(device)
                                                delegate = self.__notify_handler(self.__devices_around[device],
                                                                                 characteristic.handle)
                                                response = delegate.data
                                        if response is not None:
                                            log.debug('Response from device: %s', response)
                                            if requests['withResponse']:
                                                response = 'success'
                                            self.__gateway.send_rpc_reply(content['device'], content['data']['id'],
                                                                          str(response))
                                else:
                                    self.__log_and_send_error(
                                        'Method for rpc request - not supported by characteristic or not found in the config.\nDevice: %s with data: %s and config: %s',
                                        device,
                                        content,
                                        self.__devices_around[device]['device_config']["serverSideRpc"])
        except Exception as e:
            self.__log_and_send_info("Failed to process server-side-rpc")
            log.exception(e)

    def is_connected(self):
        return self._connected

    def open(self):
        self.__stopped = False
        self.start()

    def device_add(self, device):
        log.debug('Device with address: %s - found.', device.addr.upper())
        self.__devices_scanned.append(device)
        for interested_device in self.__devices_around:
            if device.addr.upper() == interested_device:
                if self.__devices_around[interested_device].get('scanned_device') is None:
                    self.__devices_around[interested_device]['is_new_device'] = True
                self.__devices_around[interested_device]['scanned_device'] = device
                self.__devices_around[interested_device]['is_new_rssi'] = True

    def __get_services_and_chars(self):
        valid_device_count = 0
        for device in self.__devices_around:
            if self.__devices_around.get(device) is not None and self.__devices_around[device].get('scanned_device') is not None:
                valid_device_count = valid_device_count + 1
        if valid_device_count == 0:
            self.__log_and_send_info("No BLE device configured under this gateway")
            # No device to attend, so no data to send
            self.__previous_send_data_time = time.time()
        else:
            for device in self.__devices_around:
                try:
                    if self.__devices_around.get(device) is not None and self.__devices_around[device].get(
                            'scanned_device') is not None:
                        if self.__devices_around[device].get('peripheral') is None:
                            address_type = self.__devices_around[device]['device_config'].get('addrType', "public")
                            peripheral = Peripheral(self.__devices_around[device]['scanned_device'], address_type)
                            self.__devices_around[device]['peripheral'] = peripheral
                        else:
                            peripheral = self.__devices_around[device]['peripheral']
                        try:
                            peripheral.getState()
                        except BTLEInternalError:
                            log.info('Connecting to device: %s', device)
                            peripheral.connect(self.__devices_around[device]['scanned_device'])
                        try:
                            services = peripheral.getServices()
                        except BTLEDisconnectError:
                            self.__check_and_reconnect(device)
                            services = peripheral.getServices()
                        for service in services:
                            if self.__devices_around[device].get('services') is None:
                                log.debug('Building device %s map, it may take a time, please wait...', device)
                                self.__devices_around[device]['log_device_map'] = True
                                self.__devices_around[device]['services'] = {}
                            service_uuid = str(service.uuid).upper()
                            if self.__devices_around[device]['services'].get(service_uuid) is None:
                                self.__devices_around[device]['services'][service_uuid] = {}

                                try:
                                    characteristics = service.getCharacteristics()
                                except BTLEDisconnectError:
                                    self.__check_and_reconnect(device)
                                    characteristics = service.getCharacteristics()

                                if self.__config.get('buildDevicesMap', False):
                                    for characteristic in characteristics:
                                        characteristic_uuid = str(characteristic.uuid).upper()
                                        descriptors = []
                                        self.__check_and_reconnect(device)
                                        try:
                                            descriptors = characteristic.getDescriptors()
                                        except BTLEDisconnectError:
                                            self.__check_and_reconnect(device)
                                            descriptors = characteristic.getDescriptors()
                                        except BTLEGattError as e:
                                            self.__log_and_send_error("GattError while reading char[%s] of device %s", characteristic_uuid, device)
                                            log.debug(e)
                                        except Exception as e:
                                            self.__log_and_send_error("Exception while reading char[%s] of device %s", characteristic_uuid, device)
                                            log.exception(e)
                                        if self.__devices_around[device]['services'][service_uuid].get(
                                                characteristic_uuid) is None:
                                            self.__check_and_reconnect(device)
                                            self.__devices_around[device]['services'][service_uuid][characteristic_uuid] = {'characteristic': characteristic,
                                                                                                                            'handle': characteristic.handle,
                                                                                                                            'descriptors': {}}
                                        for descriptor in descriptors:
                                            log.debug(descriptor.handle)
                                            log.debug(str(descriptor.uuid))
                                            log.debug(str(descriptor))
                                            self.__devices_around[device]['services'][service_uuid][
                                                characteristic_uuid]['descriptors'][descriptor.handle] = descriptor
                                else:
                                    for characteristic in characteristics:
                                        characteristic_uuid = str(characteristic.uuid).upper()
                                        self.__devices_around[device]['services'][service_uuid][characteristic_uuid] = {
                                            'characteristic': characteristic,
                                            'handle': characteristic.handle}

                        if self.__devices_around[device]['log_device_map']:
                            self.__devices_around[device]['log_device_map'] = False
                            log.debug('# Device : ' + device)
                            for service in self.__devices_around[device]['services']:
                                log.debug(' > Service {' + service + '}')
                                for char_uuid in self.__devices_around[device]['services'][service]:
                                    log.debug('   > Char {' + char_uuid + '}')

                        if self.__devices_around[device]['is_new_device']:
                            log.debug('New device %s - processing.', device)
                            self.__devices_around[device]['is_new_device'] = False
                            self.__new_device_processing(device)
                            self.__storage.clear(self.__devices_around[device]['device_config'])
                            for interest_char in self.__devices_around[device]['interest_uuid']:
                                for section in self.__devices_around[device]['interest_uuid'][interest_char]:
                                    if section['section_config'].get('onceOnConnect') is True:
                                        data = self.__service_processing(device, section['section_config'])
                                        converter = section['converter']
                                        converted_data = converter.convert(section, data)
                                        self.statistics['MessagesReceived'] = self.statistics['MessagesReceived'] + 1
                                        if converted_data is not None:
                                            self.__storage.update(section, converted_data)
                            self.__send_storage()

                        self.__storage.clear(self.__devices_around[device]['device_config'])
                        for interest_char in self.__devices_around[device]['interest_uuid']:
                            for section in self.__devices_around[device]['interest_uuid'][interest_char]:
                                if section['section_config'].get('onceOnConnect') is not True:
                                    data = self.__service_processing(device, section['section_config'])
                                    if data is not None:
                                        converter = section['converter']
                                        converted_data = converter.convert(section, data)
                                        self.statistics['MessagesReceived'] = self.statistics['MessagesReceived'] + 1
                                        if converted_data is not None:
                                            self.__storage.update(section, converted_data)

                        if not self.__storage.is_empty() and self.__rssi_as_telemetry is not None:
                            if self.__devices_around[device]['is_new_rssi']: # Recording RSSI of the device
                                self.__storage.update_telemetry(self.__rssi_as_telemetry, self.__devices_around[device]['scanned_device'].rssi)
                                self.__devices_around[device]['is_new_rssi'] = False
                        self.__send_storage()

                        if self.__disconnect_after_use and peripheral is not None:
                            self.__log_and_send_info('Disconnecting device %s after use ', device)
                            try:
                                peripheral.disconnect()
                            except Exception as e:
                                self.__log_and_send_error('Unable to disconnect ' + device)
                                log.exception(e)

                except BTLEDisconnectError as e:
                    self.__log_and_send_info("Connection lost. Device " + device + " (or unable to connect)")
                    log.exception(e)
                except Exception as e:
                    self.__log_and_send_info("Exception while reading from Device " + device)
                    log.exception(e)

    def __new_device_processing(self, device):
        default_services_on_device = [service for service in self.__devices_around[device]['services'].keys() if
                                      int(service.split('-')[0], 16) in self.__default_services]
        log.debug('Default services found on device %s :%s', device, default_services_on_device)
        converter = BytesBLEUplinkConverter()
        converted_data = None
        self.__storage.clear(self.__devices_around[device]['device_config'])
        for service in default_services_on_device:
            characteristics = [char for char in self.__devices_around[device]['services'][service].keys() if
                               self.__devices_around[device]['services'][service][char][
                                   'characteristic'].supportsRead()]
            for char in characteristics:
                read_config = {'characteristicUUID': char,
                               'method': 'READ',
                               }
                try:
                    self.__check_and_reconnect(device)
                    data = self.__service_processing(device, read_config)
                    attribute = capitaliseName(UUID(char).getCommonName())
                    read_config['key'] = attribute
                    read_config['byteFrom'] = 0
                    read_config['byteTo'] = -1
                    converter_config = [{"type": "attributes",
                                         "section_config": read_config}]
                    for interest_information in converter_config:
                        try:
                            converted_data = converter.convert(interest_information, data)
                            self.statistics['MessagesReceived'] = self.statistics['MessagesReceived'] + 1
                            if converted_data is not None:
                                # log.debug("Storage updating \n%s \n%s",
                                #     simplejson.dumps(interest_information, sort_keys=True, indent=2),
                                #     simplejson.dumps(converted_data, sort_keys=True, indent=2))
                                self.__storage.update(interest_information, converted_data)
                        except Exception as e:
                            log.debug(e)
                except Exception as e:
                    log.debug('Cannot process %s', e)
                    continue
        self.__send_storage()

    def __check_and_reconnect(self, device):
        # pylint: disable=protected-access
        for i in range(self.reconnect_try_count):
            if self.__devices_around[device]['peripheral']._helper is None:
                log.info("ReConnecting#%i to %s...", i, device)
                self.__devices_around[device]['peripheral'].connect(self.__devices_around[device]['scanned_device'])

    def __notify_handler(self, device, notify_handle, delegate=None):
        class NotifyDelegate(DefaultDelegate):
            def __init__(self):
                DefaultDelegate.__init__(self)
                self.device = device
                self.data = {}

            def handleNotification(self, handle, data):
                self.data = data
                log.debug('Notification received from device %s handle: %i, data: %s', self.device, handle, data)

        if delegate is None:
            delegate = NotifyDelegate()
        device['peripheral'].withDelegate(delegate)
        device['peripheral'].writeCharacteristic(notify_handle, b'\x01\x00', True)
        if device['peripheral'].waitForNotifications(1):
            log.debug("Data received: %s", delegate.data)
        return delegate

    def __service_processing(self, device, characteristic_processing_conf):
        characteristic_uuid_from_config = characteristic_processing_conf.get('characteristicUUID')
        if characteristic_uuid_from_config is None:
            self.__log_and_send_error('Characteristic not found in config: %s', pformat(characteristic_processing_conf))
            return None
        for service in self.__devices_around[device]['services']:
            if self.__devices_around[device]['services'][service].get(characteristic_uuid_from_config) is None:
                continue
            characteristic = self.__devices_around[device]['services'][service][characteristic_uuid_from_config][
                'characteristic']
            self.__check_and_reconnect(device)
            data = None
            if characteristic_processing_conf.get('method', '_').upper().split()[0] == "READ":
                if characteristic.supportsRead():
                    self.__check_and_reconnect(device)
                    data = characteristic.read()
                else:
                    self.__log_and_send_error('This characteristic doesn\'t support "READ" method.')
            if characteristic_processing_conf.get('method', '_').upper().split()[0] == "NOTIFY":
                self.__check_and_reconnect(device)
                descriptor = characteristic.getDescriptors(forUUID=0x2902)[0]
                handle = descriptor.handle
                if self.__notify_delegators.get(device) is None:
                    self.__notify_delegators[device] = {}
                if self.__notify_delegators[device].get(handle) is None:
                    self.__notify_delegators[device][handle] = {'function': self.__notify_handler,
                                                                'args': (self.__devices_around[device],
                                                                         handle,
                                                                         self.__notify_delegators[device].get(handle)),
                                                                'delegate': None
                                                                }
                    self.__notify_delegators[device][handle]['delegate'] = self.__notify_delegators[device][handle][
                        'function'](*self.__notify_delegators[device][handle]['args'])
                    data = self.__notify_delegators[device][handle]['delegate'].data
                else:
                    self.__notify_delegators[device][handle]['args'] = (self.__devices_around[device],
                                                                        handle,
                                                                        self.__notify_delegators[device][handle]['delegate'])
                    self.__notify_delegators[device][handle]['delegate'] = self.__notify_delegators[device][handle][
                        'function'](*self.__notify_delegators[device][handle]['args'])
                    data = self.__notify_delegators[device][handle]['delegate'].data
            if data is None:
                self.__log_and_send_error('Cannot process characteristic: %s with config:\n%s', str(characteristic.uuid).upper(),
                          pformat(characteristic_processing_conf))
            else:
                log.debug('data: %s', data)
            return data
        self.__log_and_send_error('Characteristic [%s] not found', characteristic_uuid_from_config)
        return None

    def __reset_hardware(self):
        if not sys.platform.startswith('linux'):
            self.__log_and_send_info('Resetting BLE hardware is not yet implemented for OS other then Linux')
            return
        self.__log_and_send_info("Resetting BLE hardware...")
        devices = ''
        try:
            devices = subprocess.check_output(['hciconfig'], timeout=1, encoding='ascii')
        except Exception as e:
            self.__log_and_send_error('Unable to get hardware list using hcitool command')
            log.exception(e)

        # === Sample output            
        # pi@raspberrypi:/ $ hciconfig
        # hci0:   Type: Primary  Bus: USB
        #         BD Address: 00:00:00:00:00:00  ACL MTU: 0:0  SCO MTU: 0:0
        #         DOWN
        #         RX bytes:284 acl:0 sco:0 events:4 errors:0
        #         TX bytes:15 acl:0 sco:0 commands:5 errors:0

        devices = devices.split("\n")   # prepare all lines
        devices = [d for d in devices if d and not d.startswith('\t')]  # consider lines which is not starting with <tab> (means it has device name)
        devices = [d[:d.find(':')] for d in devices if d.find(':') >= 0]    # filter device name

        if len(devices) > 0:
            log.debug('"hciconfig" hardware found = %s', ', '.join(devices))
            for device in devices:
                try:
                    reset_output = subprocess.check_output(['sudo', 'hciconfig', device, 'reset'], timeout=2, encoding='ascii')
                    self.__log_and_send_info('BLE hardware "%s" reset successfully', device)
                except Exception as e:
                    self.__log_and_send_error('Failed to reset hardware "%s"', device)
                    log.exception(e)
        else:
            self.__log_and_send_error("No hci hardware detected!")
            log.debug('No hardware detected!')

    def __scan_ble(self):
        self.__devices_scanned.clear() # clear scanned device list before begin
        if self.__disconnect_before_scan:
            disconnect_invoked = False
            for device in self.__devices_around:
                if self.__devices_around[device].get('peripheral') is not None:
                    peripheral = self.__devices_around[device]['peripheral']
                    if peripheral is not None:
                        self.__log_and_send_info('Disconnecting device %s before scanning', device)
                        try:
                            peripheral.disconnect()
                            disconnect_invoked = True
                        except Exception as e:
                            self.__log_and_send_error('Unable to disconnect %s', device)
                            log.exception(e)
            if disconnect_invoked and self.__disconnect_safe_wait > 0:
                self.__log_and_send_info('Waiting for %d seconds to disconnect safely...', self.__disconnect_safe_wait)
                time.sleep(self.__disconnect_safe_wait)

        self.__log_and_send_info("Scanning for BLE devices...")
        try:
            self.__scanner.scan(self.__config.get('scanTimeSeconds', 5),
                                passive=self.__config.get('passiveScanMode', False))
            self.__log_and_send_info("Scanning successfully finished")
        except BTLEManagementError as e:
            log.debug('BLE working only with root user.')
            log.debug('Or you can try this command:\nsudo setcap '
                      '\'cap_net_raw,cap_net_admin+eip\' %s'
                      '\n====== Attention! ====== '
                      '\nCommand above - provided access to ble devices to any user.'
                      '\n========================', str(bluepy_path[0] + '/bluepy-helper'))
            self._connected = False
            log.debug("Unable to scan devices. Further operations may fail. Try to restart or reconnect your BLE hardware.")
            self.__log_and_send_error("Scanning not finished successfully (ble-management error, require hardware reset)")
            if len(self.__devices_scanned) <= 0:
                self.__reset_hardware()
            # raise e # Don't raise exception to avoid permanent blocking of the system!
        except Exception as e:
            log.exception(e)
            self.__log_and_send_error("Scanning not finished successfully (unknown exception, require hardware reset)")
            if len(self.__devices_scanned) <= 0:
                self.__reset_hardware()
        self.__write_scanned_device_list()
        # FIXME: Huge number of nearby ble devices might got timeout even earliar than
        #        some really present device being scanned.
        #        In that case system never connect to such device - which is wrong.
        #        So lets keep this scaning process just to list out found devices only.
        #        And later on try to connect all listed device to really see if it found or not.
        for device in self.__devices_around:
            if self.__devices_around[device].get('scanned_device') is None:
                self.__devices_around[device]['scanned_device'] = device
                self.__devices_around[device]['is_new_device'] = True

    def __write_scanned_device_list(self):
        if self.__filepath_scanned_device_list is not None:
            lst_scanned_devices = []
            log.debug("Writing scanned-device list to '" + self.__filepath_scanned_device_list + "'")
            try:
                for device in self.__devices_scanned:
                    # interest_device = self.__devices_around.get(device)
                    # refer https://www.libelium.com/forum/libelium_files/bt4_core_spec_adv_data_reference.pdf
                    # refer https://github.com/ARMmbed/ble/blob/master/ble/GapAdvertisingData.h
                    lst_scanned_devices.append({"short_name": device.getValueText(8),"name": device.getValueText(9),"mac": device.addr.upper(),"rssi": device.rssi})
                str_json = simplejson.dumps(lst_scanned_devices, sort_keys=True, indent=2)
                with open(self.__filepath_scanned_device_list, 'w') as file_json:
                    file_json.write(str_json)
            except Exception as e:
                log.warn("Failed writing scanned-device list to json file")
                log.exception(e)


    def __fill_interest_devices(self):
        if self.__config.get('devices') is None:
            self.__send_error("Devices not found in configuration file. BLE Connector stopped")
            self._connected = False
            return None
        for interest_device in self.__config.get('devices'):
            keys_in_config = ['attributes', 'telemetry']
            if interest_device.get('MACAddress') is not None:
                default_converter = BytesBLEUplinkConverter()
                interest_uuid = {}
                for key_type in keys_in_config:
                    for type_section in interest_device.get(key_type):
                        if type_section.get("characteristicUUID") is not None:
                            type_section["characteristicUUID"] = type_section["characteristicUUID"].upper()
                            converter = None
                            if type_section.get('converter') is not None:
                                try:
                                    module = TBUtility.check_and_import(self.__connector_type,
                                                                        type_section['converter'])
                                    if module is not None:
                                        log.debug('Custom converter for device %s - found!',
                                                  interest_device['MACAddress'])
                                        converter = module()
                                    else:
                                        self.__log_and_send_error(
                                            "\n\nCannot find extension module for device %s .\nPlease check your configuration.\n",
                                            interest_device['MACAddress'])
                                except Exception as e:
                                    log.exception(e)
                            else:
                                converter = default_converter
                            if converter is not None:
                                if interest_uuid.get(type_section["characteristicUUID"]) is None:
                                    interest_uuid[type_section["characteristicUUID"]] = [
                                        {'section_config': type_section,
                                         'type': key_type,
                                         'converter': converter}]
                                else:
                                    interest_uuid[type_section["characteristicUUID"]].append(
                                        {'section_config': type_section,
                                         'type': key_type,
                                         'converter': converter})
                        else:
                            self.__log_and_send_error("No characteristicUUID found in configuration section for %s:\n%s\n", key_type,
                                      pformat(type_section))
                if self.__devices_around.get(interest_device['MACAddress'].upper()) is None:
                    self.__devices_around[interest_device['MACAddress'].upper()] = {}
                self.__devices_around[interest_device['MACAddress'].upper()]['device_config'] = interest_device
                self.__devices_around[interest_device['MACAddress'].upper()]['interest_uuid'] = interest_uuid
            else:
                self.__log_and_send_error("Device address not found, please check your settings. (device-name=%s)", interest_device.get('name'))

    def __perform_system_reboot(self):
        try:
            self.__log_and_send_info('Performing system reboot...')
            reset_output = subprocess.check_output(['sudo', 'reboot'], timeout=2, encoding='ascii')
        except Exception as e:
            self.__log_and_send_error('Failed to perform system reboot!')
            log.exception(e)

    def __send_storage(self):
        if self.__storage:
            if not self.__storage.is_empty():
                log.debug("Sending data to storage: %s", self.__storage.get())
                try:
                    self.__gateway.send_to_storage(self.get_name(), self.__storage.get())
                except Exception as e:
                    log.error("Error sending data to storage: %s", self.__storage.get())
                    log.exception(e)
                self.statistics['MessagesSent'] = self.statistics['MessagesSent'] + 1
                self.__previous_send_data_time = time.time()

    def __send_log_as_telemetry(self, type, message):
        if self.__log_as_telemetry:
            try:
                self.__gateway.send_to_storage(self.__gateway.name, {"deviceName": self.__gateway.name, "telemetry": [{"ble_log": (type + ": " + message)}]})
            except Exception as e:
                log.exception(e)

    def __log_and_send_error(self, message):
        log.error(message)
        self.__send_log_as_telemetry("ERROR", message)

    def __log_and_send_info(self, message):
        log.info(message)
        self.__send_log_as_telemetry("INFO", message)

class ScanDelegate(DefaultDelegate):
    def __init__(self, ble_connector):
        DefaultDelegate.__init__(self)
        self.__connector = ble_connector

    def handleDiscovery(self, dev, is_new_device, _):
        if is_new_device:
            self.__connector.device_add(dev)
