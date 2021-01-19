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

from thingsboard_gateway.connectors.ble.ble_uplink_converter import BLEUplinkConverter, log


class BytesBLEUplinkConverter(BLEUplinkConverter):
    def __init__(self):
        pass

    def convert(self, config, data):
        try:
            try:
                byte_from = config['section_config'].get('byteFrom')
                byte_to = config['section_config'].get('byteTo')
                try:
                    if data is None:
                        return {}
                    byte_to = byte_to if byte_to != -1 else len(data)
                    converted_data = data[byte_from: byte_to]
                    try:
                        converted_data = converted_data.replace(b"\x00", b'').decode('UTF-8')
                    except UnicodeDecodeError:
                        converted_data = str(converted_data)
                    log.debug('converted_data: %s', converted_data)
                    return converted_data
                except Exception as e:
                    log.error('\nException catched when processing data for %s\n\n', pformat(config))
                    log.exception(e)
            except Exception as e:
                log.exception(e)
        except Exception as e:
            log.exception(e)
        return None
