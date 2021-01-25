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
        self.__filepath_scanned_device_list = self.__config['scannedDeviceListFilepath'] if self.__config.get(
            'scannedDeviceListFilepath') is not None else None
        self.__previous_scan_time = time.time() - self.__rescan_time
        self.__previous_read_time = time.time() - self.__check_interval_seconds
        self.__scanner = Scanner().withDelegate(ScanDelegate(self))
        self.__devices_around = {}
        self.__devices_scanned = []
        self.__available_converters = []
        self.__notify_delegators = {}
        self.__fill_interest_devices()
        self.daemon = True

    def run(self):
        while True:
            if time.time() - self.__previous_scan_time >= self.__rescan_time != 0:
                self.__scan_ble()
                self.__previous_scan_time = time.time()

            if time.time() - self.__previous_read_time >= self.__check_interval_seconds:
                self.__get_services_and_chars()
                self.__previous_read_time = time.time()

            time.sleep(.1)
            if self.__stopped:
                log.debug('STOPPED')
                break

    def close(self):
        self.__stopped = True
        for device in self.__devices_around:
            try:
                if self.__devices_around[device].get('peripheral') is not None:
                    self.__devices_around[device]['peripheral'].disconnect()
            except Exception as e:
                log.exception(e)
                log.error("Unable to close the device. Further operations may fail. Try to restart or reconnect your BLE hardware.")
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
                                        log.exception(e)
                            else:
                                log.error(
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
                                    log.error(
                                        'Method for rpc request - not supported by characteristic or not found in the config.\nDevice: %s with data: %s and config: %s',
                                        device,
                                        content,
                                        self.__devices_around[device]['device_config']["serverSideRpc"])
        except Exception as e:
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
            if device.addr.upper() == interested_device and self.__devices_around[interested_device].get(
                    'scanned_device') is None:
                self.__devices_around[interested_device]['scanned_device'] = device
                self.__devices_around[interested_device]['is_new_device'] = True

    def __get_services_and_chars(self):
        for device in self.__devices_around:
            try:
                if self.__devices_around.get(device) is not None and self.__devices_around[device].get(
                        'scanned_device') is not None:
                    log.debug('Connecting to device: %s', device)
                    if self.__devices_around[device].get('peripheral') is None:
                        address_type = self.__devices_around[device]['device_config'].get('addrType', "public")
                        peripheral = Peripheral(self.__devices_around[device]['scanned_device'], address_type)
                        self.__devices_around[device]['peripheral'] = peripheral
                    else:
                        peripheral = self.__devices_around[device]['peripheral']
                    try:
                        log.info(peripheral.getState())
                    except BTLEInternalError:
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
                                    descriptors = []
                                    self.__check_and_reconnect(device)
                                    try:
                                        descriptors = characteristic.getDescriptors()
                                    except BTLEDisconnectError:
                                        self.__check_and_reconnect(device)
                                        descriptors = characteristic.getDescriptors()
                                    except BTLEGattError as e:
                                        log.debug(e)
                                    except Exception as e:
                                        log.exception(e)
                                    characteristic_uuid = str(characteristic.uuid).upper()
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
                        if not self.__storage.is_empty():
                            log.debug("Sending data to storage: %s", self.__storage.get())
                            self.__gateway.send_to_storage(self.get_name(), self.__storage.get())
                            self.statistics['MessagesSent'] = self.statistics['MessagesSent'] + 1

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

                    if not self.__storage.is_empty():
                        log.debug("Sending data to storage: %s", self.__storage.get())
                        self.__gateway.send_to_storage(self.get_name(), self.__storage.get())
                        self.statistics['MessagesSent'] = self.statistics['MessagesSent'] + 1

                    if self.__disconnect_after_use and peripheral is not None:
                        log.debug('Disconnecting ' + device)
                        try:
                            peripheral.disconnect()
                        except Exception as e:
                            log.debug('Unable to disconnect ' + device)
                            log.exception(e)

            except BTLEDisconnectError:
                log.debug('Connection lost. Device %s', device)
                continue
            except Exception as e:
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
                                self.__storage.update(interest_information, converted_data)
                        except Exception as e:
                            log.debug(e)
                except Exception as e:
                    log.debug('Cannot process %s', e)
                    continue
        if converted_data is not None:
            # self.__gateway.add_device(converted_data["deviceName"], {"connector": self})
            log.debug("Sending data to storage: %s", self.__storage.get())
            self.__gateway.send_to_storage(self.get_name(), self.__storage.get())
            self.statistics['MessagesSent'] = self.statistics['MessagesSent'] + 1

    def __check_and_reconnect(self, device):
        # pylint: disable=protected-access
        while self.__devices_around[device]['peripheral']._helper is None:
            log.debug("Connecting to %s...", device)
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
            log.error('Characteristic not found in config: %s', pformat(characteristic_processing_conf))
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
                    log.error('This characteristic doesn\'t support "READ" method.')
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
                log.error('Cannot process characteristic: %s with config:\n%s', str(characteristic.uuid).upper(),
                          pformat(characteristic_processing_conf))
            else:
                log.debug('data: %s', data)
            return data
        log.error('Characteristic [%s] not found', characteristic_uuid_from_config)
        return None

    def __reset_hardware(self):
        if not sys.platform.startswith('linux'):
            log.debug('Resetting BLE hardware is not yet implemented for OS other then Linux')
            return
        log.info('Resetting BLE hardware...')
        devices = ''
        try:
            devices = subprocess.check_output(['hciconfig'], timeout=1, encoding='ascii')
        except Exception as e:
            log.error('Unable to get hardware list using hcitool command')
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
                    log.info('BLE hardware "%s" reset successfully', device)
                except Exception as e:
                    log.error('Failed to reset hardware "%s"', device)
                    log.exception(e)
        else:
            log.debug('No hardware detected!')

    def __scan_ble(self):
        self.__devices_scanned.clear() # clear scanned device list before begin
        if self.__disconnect_before_scan:
            log.debug("Disconnect devices before scanning...")
            disconnect_invoked = False
            for device in self.__devices_around:
                if self.__devices_around[device].get('peripheral') is not None:
                    peripheral = self.__devices_around[device]['peripheral']
                    if peripheral is not None:
                        log.debug('Disconnecting ' + device)
                        try:
                            peripheral.disconnect()
                            disconnect_invoked = True
                        except Exception as e:
                            log.debug('Unable to disconnect ' + device)
                            log.exception(e)
            if disconnect_invoked and self.__disconnect_safe_wait > 0:
                log.debug('Waiting for %d seconds to disconnect safely...', self.__disconnect_safe_wait)
                time.sleep(self.__disconnect_safe_wait)

        log.debug("Scanning for devices...")
        try:
            self.__scanner.scan(self.__config.get('scanTimeSeconds', 5),
                                passive=self.__config.get('passiveScanMode', False))
        except BTLEManagementError as e:
            log.error('BLE working only with root user.')
            log.error('Or you can try this command:\nsudo setcap '
                      '\'cap_net_raw,cap_net_admin+eip\' %s'
                      '\n====== Attention! ====== '
                      '\nCommand above - provided access to ble devices to any user.'
                      '\n========================', str(bluepy_path[0] + '/bluepy-helper'))
            self._connected = False
            log.error("Unable to scan devices. Further operations may fail. Try to restart or reconnect your BLE hardware.")
            self.__reset_hardware()
            # raise e # Don't raise exception to avoid permanent blocking of the system!
        except Exception as e:
            log.exception(e)
            self.__reset_hardware()
        self.__write_scanned_device_list()

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
            log.error('Devices not found in configuration file. BLE Connector stopped.')
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
                                        log.error(
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
                            log.error("No characteristicUUID found in configuration section for %s:\n%s\n", key_type,
                                      pformat(type_section))
                if self.__devices_around.get(interest_device['MACAddress'].upper()) is None:
                    self.__devices_around[interest_device['MACAddress'].upper()] = {}
                self.__devices_around[interest_device['MACAddress'].upper()]['device_config'] = interest_device
                self.__devices_around[interest_device['MACAddress'].upper()]['interest_uuid'] = interest_uuid
            else:
                log.error("Device address not found, please check your settings.")


class ScanDelegate(DefaultDelegate):
    def __init__(self, ble_connector):
        DefaultDelegate.__init__(self)
        self.__connector = ble_connector

    def handleDiscovery(self, dev, is_new_device, _):
        if is_new_device:
            self.__connector.device_add(dev)
