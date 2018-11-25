import requests
import json as js
import csv


headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'X-Consumer-Custom-ID': 'f2be76c2-93ca-4152-9de5-0d78a3343f16',
}

response = requests.post('http://192.168.60.55:4465/bigbrain/eai-explorer/eai/38bad7cf-c82b-496f-a050-dcbcc1608e2a/submit/un-auth/pipeline/38bad7cf-c82b-496f-a050-dcbcc1608e2a/api', headers=headers)
print(response.status_code)

headers = {
    'accept': '*/*',
    'Content-Type': 'application/json',
}

if response.status_code != 404:

    json_response_string = response.text
    parsed_json = js.loads(json_response_string)

    blocklist = parsed_json["result"]["pipelineList"][0]["blockList"]
    operatorlist = parsed_json["result"]['pipelineList'][0]['operatorList']
    #print(operatorlist)
    connectorlist = parsed_json["result"]['pipelineList'][0]['connectionList']


    block_list = open('/Users/sajesh/blockdetails/blocks.csv', 'w')
    operator_list = open('/Users/sajesh/blockdetails/operators.csv', 'w')
    connector_list = open('/Users/sajesh/blockdetails/connectors.csv', 'w')
    pipeline_list = open('/Users/sajesh/blockdetails/pipeline.csv', 'w')

    csvblockwriter = csv.writer(block_list)
    csvoperatorwriter = csv.writer(operator_list)
    csvconnectorwriter = csv.writer(connector_list)
    csvpipelinewrite = csv.writer(pipeline_list)

    count = 0

    pipelineid=''
    stagename=''
    stageid=''
    environment=''
    blockid=''
    operatorid=''
    operatorname=''

    for blocks in blocklist:
        if count == 0:
            header = blocks.keys()
            csvblockwriter.writerow(header)
            count += 1

        blockid = blocks['id']
        blockname=blocks['name']
        stagename = blocks['parentName']
        stageid = blocks['parentInstance']
        environment= blocks['env']
        mode=blocks['mode']
        cpucores=blocks['userSpec']['cores']
        memoryvalue=['userSpec']['memory']['value']
        memoryunit=['userSpec']['memory']['unit']
        memoryused=memoryvalue+' '+memoryunit

        dependencies = blocks['dependencies']
        dependencydetails = []


        if dependencies != None:
            if len(dependencies)==0:
                continue

           # data = '{ "libraryIdList": [ "262b36d1-a1d2-47bc-bc01-3e6b215f184f", "4aceea67-69fd-4570-b56f-75bf95a2d9bb", "f8e2e2b0-2391-4615-9dfb-52237e37e571" ]}'
            data = '{ "libraryIdList": ' + str(dependencies) + '}'
            data = data.replace("\'","\"")
            response = requests.post(
                'https://192.168.60.55:18002/bigbrain/administration/library-manager/un-auth/library-meta-info',
                headers=headers, data=data, verify=False)
            json_dependency_string = response.text
            dependency_json = js.loads(json_dependency_string)

            dependencylist = dependency_json["result"]["libraryInfoMap"]

            dependencydetails = []


            for dependenciesmod in dependencies:
                #print(dependenciesmod)
                dependencydetails.append(dependencylist[dependenciesmod])

        #print(dependencydetails)
        targets =[]
        sources = []

        for connector_source_target in connectorlist:
            if(blockid == connector_source_target['source']['id']):
                targets.append(connector_source_target['target']['id'])
            if (blockid == connector_source_target['target']['id']) :
                sources.append(connector_source_target['source']['id'])
        #print("Block id")
        #print(blockid)
        #print("Source : Target")
        #print(sources)
        #print(targets)

        csvblockwriter.writerow(blocks.values())
        csvpipelinewrite.writerow([blockid,blockname,stagename,stageid,environment,sources,targets,dependencydetails,cpucores,memoryused])

    #exit()
    count = 0
    for operators in operatorlist:
        if count == 0:
            header = operators.keys()
            csvoperatorwriter.writerow(header)
            count += 1

        operatorid = operators['id']
        operatorname = operators['nodeType']
        stagename = operators['parentName']
        stageid = operators['parentInstance']
        environment = operators['name']

        target = operators['source']
        source = operators['target']

        sources = []
        targets = []

        for sourcelist in source:
            sources.append(sourcelist['id'])

        for targetlist in target:
            targets.append(targetlist['id'])

        csvoperatorwriter.writerow(operators.values())
        #csvpipelinewrite.writerow(operatorid + ',' + str(stagename) + ',' + str(stageid) + ',' + str(environment))
        csvpipelinewrite.writerow([operatorid, operatorname, stagename, stageid, environment,sources,targets,''])

    count = 0
    for connectors in connectorlist:
        if count == 0:
            header = connectors.keys()
            csvconnectorwriter.writerow(header)
            count += 1

        csvconnectorwriter.writerow(connectors.values())

