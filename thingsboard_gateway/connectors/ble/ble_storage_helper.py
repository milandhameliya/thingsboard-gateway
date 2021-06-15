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

from pprint import pformat

from thingsboard_gateway.connectors.ble.ble_uplink_converter import log

class BLEStorageHelper():
    def __init__(self):
        self.dict_result = {"deviceName": "",
                            "deviceType": "",
                            "telemetry": [],
                            "attributes": []
                            }

    def get(self):
        return self.dict_result

    def clear(self, config):
        self.dict_result["deviceName"] = ""
        self.dict_result["deviceType"] = ""
        self.dict_result["telemetry"] = []
        self.dict_result["attributes"] = []
        if config is not None:
            self.dict_result["deviceName"] = config.get('name', config['MACAddress'])
            self.dict_result["deviceType"] = config.get('deviceType', 'BLEDevice')

    def is_empty(self):
        if len(self.dict_result["telemetry"]) == 0:
            if len(self.dict_result["attributes"]) == 0:
                return True
        return False

    def update(self, config, data):
        if self.dict_result.get(config['type']) is not None:
            if config['section_config'].get('key') is not None:
                self.dict_result[config['type']].append({config['section_config'].get('key'): data})
            else:
                log.error('Key for %s not found in config: %s', config['type'], config['section_config'])
        else:
            log.error("Given type '%s' not valid for storage", config['type'])

    def update_attribute(self, key, data):
        if self.dict_result.get("attributes") is not None:
            if key is not None:
                self.dict_result["attributes"].append({key: data})
            else:
                log.error('Key for %s not found for attribute', key)
        else:
            log.error("Given attribute not valid for storage", type)

    def update_telemetry(self, key, data):
        if self.dict_result.get("telemetry") is not None:
            if key is not None:
                self.dict_result["telemetry"].append({key: data})
            else:
                log.error('Key for %s not found for telemetry', key)
        else:
            log.error("Given telemetry not valid for storage", type)
