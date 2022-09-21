import json
import os

from os import listdir
from os.path import isfile, join
import shutil

from report_data_storage import KustoConnector

'''
for more detail, you can see
https://dev.azure.com/msazure/AzureWiki/_wiki/wikis/AzureWiki.wiki/373699/sairedis-log-scanner
'''


ngsdevice_type = "ToRRouter"
sai_path = "/data/sonic-mgmt-int/SAI/inc/"
json_log_path = "/data/sonic-mgmt-int/test_reporting/test"
sai_obj_feature_map = {}
swss_device_log_items = {
    "/data/sonic-mgmt-int/test_reporting/tmp/AC_iad20-0101-0903-12t0": {
        'os_version': '20181130.101',
        'deployment_type':  "AzureAll",
        'deployment_subtype': 'Compute',
        'device': 'iad20-0101-0903-12t0',
    },
    "/data/sonic-mgmt-int/test_reporting/tmp/AC_iad20-0101-0903-16t0": {
        'os_version': '20181130.101',
        'deployment_type':  "AzureAll",
        'deployment_subtype': 'Compute',
        'device': 'iad20-0101-0903-16t0',
    },
    "/data/sonic-mgmt-int/test_reporting/tmp/AO_lvl02-0101-0630-06t0": {
        'os_version': '20181130.95',
        'deployment_type':  "AzureAll",
        'deployment_subtype': 'Others',
        'device': 'lvl02-0101-0630-06t0',
    },
    "/data/sonic-mgmt-int/test_reporting/tmp/AO_lvl02-0101-0630-07t0": {
        'os_version': '20181130.95',
        'deployment_type':  "AzureAll",
        'deployment_subtype': 'Others',
        'device': 'lvl02-0101-0630-07t0',
    },
    "/data/sonic-mgmt-int/test_reporting/tmp/SC_ams11-0101-0604-12t0": {
        'os_version': '20181130.95',
        'deployment_type':  "AzureAll",
        'deployment_subtype': 'Storage',
        'device': 'ams11-0101-0604-12t0',
    },
    "/data/sonic-mgmt-int/test_reporting/tmp/SC_ams11-0101-0604-20t0": {
        'os_version': '20181130.95',
        'deployment_type':  "AzureAll",
        'deployment_subtype': 'Storage',
        'device': 'ams11-0101-0604-20t0',
    },
    "/data/sonic-mgmt-int/test_reporting/tmp/AU_cy2sch0600405ms": {
        'os_version': '20181130.95',
        'deployment_type':  "Autopilot",
        'deployment_subtype': 'Others',
        'device': 'cy2sch0600405ms',
    },
    "/data/sonic-mgmt-int/test_reporting/tmp/AU_cy2sch0600404ms": {
        'os_version': '20181130.95',
        'deployment_type':  "Autopilot",
        'deployment_subtype': 'Others',
        'device': 'cy2sch0600404ms',
    },
    "/data/sonic-mgmt-int/test_reporting/tmp/EX_osa22-0101-0210-02t0": {
        'os_version': '20191130.79',
        'deployment_type':  "Exchange",
        'deployment_subtype': 'Others',
        'device': 'osa22-0101-0210-02t0',
    },
    "/data/sonic-mgmt-int/test_reporting/tmp/EX_osa22-0101-0213-06t0": {
        'os_version': '20191130.79',
        'deployment_type':  "Exchange",
        'deployment_subtype': 'Others',
        'device': 'osa22-0101-0213-06t0',
    },
    "/data/sonic-mgmt-int/test_reporting/tmp/PS_ams09-0101-0818-12t0": {
        'os_version': '20191130.95',
        'deployment_type':  "Pilotfish",
        'deployment_subtype': 'Storage',
        'device': 'ams09-0101-0818-12t0',
    },
    "/data/sonic-mgmt-int/test_reporting/tmp/PS_ams25-0101-0714-17t0": {
        'os_version': '20191130.95',
        'deployment_type':  "Pilotfish",
        'deployment_subtype': 'Storage',
        'device': 'ams25-0101-0714-17t0',
    },
    "/data/sonic-mgmt-int/test_reporting/tmp/PO_ams21-0101-0502-13t0": {
        'os_version': '20191130.85',
        'deployment_type':  "Pilotfish",
        'deployment_subtype': 'Others',
        'device': 'ams21-0101-0502-13t0',
    },
    "/data/sonic-mgmt-int/test_reporting/tmp/PO_ams21-0101-0602-20t0": {
        'os_version': '20191130.85',
        'deployment_type':  "Pilotfish",
        'deployment_subtype': 'Others',
        'device': 'ams21-0101-0602-20t0',
    },
}

sample_log = '2021-11-12.17:41:22.444222|r|SAI_OBJECT_TYPE_ROUTE_ENTRY:{"dest":"2603:10b6:620::/45","switch_id":"oid:0x21000000000000","vr":"oid:0x3000000000042"}'
operation_map = {'r': 'remove', 'c': 'create',
                 'g': 'get', 's': 'set', 'q': 'query'}
# skip G, which is used to get the return value
operation_features = ['r', 'c', 'g', 's']


def get_files_from_path(path):
    onlyfiles = [f for f in listdir(path) if isfile(join(path, f))]
    return onlyfiles


def get_files_from_path_and_name_pattern(path, name_pattern, exclusive_pattern):
    onlyfiles = []
    for f in listdir(path):
        if isfile(join(path, f)) and name_pattern in f and exclusive_pattern not in f:
            onlyfiles.append(join(path, f))
    # for i in onlyfiles:
    #     print(i)
    return onlyfiles


def generate_sai_feature_file_map_from_header_files(files):
    map = {}
    for i in files:
        feature = i.replace('sai', '')
        feature = feature.replace('.h', '')
        if feature:
            map[feature] = i
    return map


def generate_sai_feature_from_header_files(files):
    features = []
    for i in files:
        feature = i.replace('sai', '')
        feature = feature.replace('.h', '')
        if feature:
            features.append(feature)

    return features

def get_object_type_from_log(line):
    # object always start with SAI_OBJECT_TYPE
    items = line.split('|')
    for i in items:
        if i.startswith('SAI_OBJECT_TYPE'):
            obj = i.split(':', 1)
            if (len(obj) == 1):
                return obj[0], None
            return obj[0], obj[1]

    return None


def get_log_time(line):
    # only get the time when contains SAI_OBJECT_TYPE
    # in format 2021-11-12.17:41:22.444222
    strftime = '%Y-%m-%d.%H:%M:%S.%f'
    if 'SAI_OBJECT_TYPE' in line:
        items = line.split('|')
        # return datetime.strptime(items[0], strftime)
        return items[0]
    return None


def get_sai_op(line):
    if 'SAI_OBJECT_TYPE' in line:
        items = line.split('|')
        return operation_map.get(items[1])
    return None


def get_sai_api(op, obj):
    try:
        obj = obj.replace('SAI_OBJECT_TYPE_', '').lower()
        return '_'.join([op, obj])
    except:
        print(op, obj)


def get_sai_obj_type(line):
    attributes = []
    items = line.split('|')
    for item in items:
        if '=' in item:
            attributes.append(item.replace('\n', '').split('='))
    return attributes


def get_sai_header_file_from_sai_obj(feature, sai_feature_file_map):
    if feature in sai_feature_file_map:
        file = sai_feature_file_map[feature]
    else:
        print("feature: {} not in sai_feature_file_map.".format(feature))
        return None
    return file


def get_sai_feature_from_sai_obj(sai_obj, features):
    if sai_obj in sai_obj_feature_map:
        feature = sai_obj_feature_map[sai_obj]
    else:
        type = sai_obj.replace('SAI_OBJECT_TYPE_', '')
        obj_secs = type.split('_')
        got_value = False
        for i in range(0, len(obj_secs)):
            feature = ''.join(obj_secs[0:len(obj_secs)-i]).lower()
            if feature in features:
                sai_obj_feature_map[sai_obj] = feature
                got_value = True
                break
        # add to default type.h
        if not got_value:
            feature = 'types'
            sai_obj_feature_map[sai_obj] = 'types'

    return feature


def convert_log_item(log_file, features, sai_feature_file_map,info):
    file = open(log_file, 'r')
    log_path = os.path.dirname(os.path.realpath(log_file))
    Lines = file.readlines()
    items = []
    for line in Lines:
        if 'SAI_OBJECT_TYPE' in line and get_sai_op(line):
            attributes = get_sai_obj_type(line)
            if len(attributes) == 0:
                log_item = Swss_log_item(info,
                    log_file, line, features, sai_feature_file_map)
                if log_item.sai_feature and log_item.header_file:
                    items.append(log_item)
            else:
                for attribute in attributes:
                    log_item = Swss_log_item(info,
                        log_file, line, features, sai_feature_file_map,  attribute)
                    if log_item.sai_feature and log_item.header_file:
                        items.append(log_item)
            #     print(log_item.__dict__)
            # exit()
    json_file = log_file + "." + info['device'] + ".json"
    print("write to file {}".format(json_file))
    with open(json_file, 'w') as f:
        json.dump([ob.__dict__ for ob in items], f, sort_keys=True, indent=4)
    #data = json.dumps([ob.__dict__ for ob in items], sort_keys=True, indent=4)


def ingest_json(kusto_db, log_file):
    kusto_db.upload_swss_report_file(log_file)


def generate_json_logs(log_path,info):
    file_list = get_files_from_path(sai_path)
    sai_feature_file_map = generate_sai_feature_file_map_from_header_files(
        file_list)
    features = generate_sai_feature_from_header_files(file_list)
    files = get_files_from_path_and_name_pattern(
        log_path, "sairedis.rec", ".gz")
    sum = len(files)
    count = 0
    for file in files:
        count += 1
        print("Generate json from file {}, {}/{}".format(file, count, sum))
        convert_log_item(file,
                         features, sai_feature_file_map,info)


def move_files_to_folder(src_path, src_file_pattern, src_excludes, target_folder):
    files = get_files_from_path_and_name_pattern(
        src_path, src_file_pattern, src_excludes)
    for src_file in files:
        src_file_ob_path = src_file
        dest_file_ob_path = src_file_ob_path.replace(src_path, target_folder)
        shutil.move(src_file_ob_path, dest_file_ob_path)


def ingest_json_logs(json_log_path):
    kusto_db = KustoConnector("SaiTestData")
    files = get_files_from_path_and_name_pattern(
        json_log_path, "sairedis.rec", ".gz")
    sum = len(files)
    count = 0
    for file in files:
        ingest_json(kusto_db, file)
        count += 1
        print("Ingested file {}, {}/{}".format(file, count, sum))


class Swss_log_item:

    def __init__(self, info,log_file, line, features, sai_feature_file_map, attribute=None):
        self.log_file = log_file
        self.log = line
        self.sai_obj, self.sai_object_key = get_object_type_from_log(
            line)
        self.log_time = get_log_time(line)
        self.sai_feature = get_sai_feature_from_sai_obj(
            self.sai_obj, features)
        self.header_file = get_sai_header_file_from_sai_obj(
            self.sai_feature, sai_feature_file_map)
        self.sai_op = get_sai_op(line)
        self.sai_api = get_sai_api(self.sai_op, self.sai_obj)
        self.sai_obj_attr_key = attribute[0] if attribute else None
        self.sai_obj_attr_value = attribute[1] if attribute else None
        self.device = info['device']
        self.os_version = info['os_version']
        self.deployment_type = info['deployment_type']
        self.deployment_subtype = info['deployment_subtype']
        self.ngsdevice_type = ngsdevice_type

    def dump_to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4)


if __name__ == "__main__":
    ''''
    Before run this command, need to 
    1. clone the sai repo to local disk and change sai_path
    2. set the json log generating folder and os_version in swss_device_log_items
    3. set the swss log input folders swss_log_paths
    '''
    for log_path, info in swss_device_log_items.items():
        generate_json_logs(log_path,info)
        move_files_to_folder(log_path, ".json", ".gz", json_log_path)

    ingest_json_logs(json_log_path)
