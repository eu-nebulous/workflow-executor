import os
import re

import traceback

from kubernetes import client, config
from kubernetes.client import CustomObjectsApi
from prometheus_client import start_http_server, Gauge
from utils import filter_nodes_by_label, parse_memory_to_bytes


start_http_server(int(os.environ.get('METRICS_PORT', 9999)))
class Scheduler():
    def __init__(
            self,
            argo_ip,
            argo_port,
            group = "workflow.io",
            version = "v1",
        ):

        try:
            try:
                print("Trying to load in-cluster config.", flush=True)
                config.load_incluster_config()
            except:
                print("In-cluster config not found, loading kube config from local machine.", flush=True)
                config.load_kube_config()                

            self.argo_ip = argo_ip
            self.argo_port = argo_port

            self.core_client = client.CoreV1Api()
            self.api_client = client.ApiClient()

            self.group = group
            self.v = version

            self.metrics = {}

            self.check_publish_metrics()
        except Exception as e:
            print(f"Cluster context can not be retrieved: {e}", flush=True)


    def define_metrics(self):
        workflow_workers = self.__get_crds("workflowworkers").get("items")

        if workflow_workers:
            self.metrics = {
                'nodes': {},
                'workflows': {
                    'Pending': {},
                    'Pods_pending': {},
                    'Running': {},
                    'Failed': {},
                    'Error': {},
                    'Succeeded': {},
                }
            }

            for i in workflow_workers:
                w_name = i.get('metadata').get('name')
                self.metrics.get('nodes')[w_name] = Gauge(f'{w_name}_count'.replace('-','_'), f'Number of {w_name} ')
                self.metrics.get('workflows').get('Pending')[w_name] = Gauge(f'workflow_pending_{w_name}_count'.replace('-','_'), f'Number of pending workflows for {w_name}')
                self.metrics.get('workflows').get('Pods_pending')[w_name] = Gauge(f'workflow_pods_pending_{w_name}_count'.replace('-','_'), f'Number of workflows with pending pods for {w_name}')
                self.metrics.get('workflows').get('Running')[w_name] = Gauge(f'workflow_running_{w_name}_count'.replace('-','_'), f'Number of running workflows for {w_name}')
                self.metrics.get('workflows').get('Succeeded')[w_name] = Gauge(f'workflow_succeed_{w_name}_count'.replace('-','_'), f'Number of succeed workflows for {w_name}')
                self.metrics.get('workflows').get('Error')[w_name] = Gauge(f'error_finished_{w_name}_count'.replace('-','_'), f'Number of error workflows for {w_name}')
                self.metrics.get('workflows').get('Failed')[w_name] = Gauge(f'failed_finished_{w_name}_count'.replace('-','_'), f'Number of failed workflows for {w_name}')
        
        else:
            self.metrics = {}

    def __get_workflows(self, label_selector=''):
        try:
            crd_client = CustomObjectsApi(self.api_client)

            workflows = crd_client.list_namespaced_custom_object(
                group='argoproj.io', 
                version='v1alpha1', 
                namespace='argo',
                plural='workflows',
                label_selector=label_selector,
            )
            return workflows.get('items')
        except client.ApiException as e:
            print(f"Error fetching Argo Workflows: {e}", flush=True) 
            return []

    def publish_metrics(self):
        self.label_workflow_nodes()

        for worker, size in self.workers.items():
            self.metrics.get('nodes').get(worker).set(size)
            
        for state, workers  in self.metrics.get('workflows').items():
            for worker_type, metric in workers.items():
                if metric != 'Pods_pending':
                    label_selector = f'workflow.nebulouscloud.eu/workersize={worker_type}'
                    metric.set(
                        len(self.__get_workflows(
                            label_selector+f',workflows.argoproj.io/phase={state}'
                        ))
                    )

        for worker in self.workers:
            workflows = self.__get_workflows(
                f'workflow.nebulouscloud.eu/workersize={worker},workflows.argoproj.io/phase=Running',
            )

            pending_workflows = 0
            for workflow in workflows:
                for node in workflow.get('status').get('nodes').values():
                    if node.get('type') == 'DAG':
                        if node.get('phase') == 'Pending':
                            pending_workflows += 1
                            break
                    elif node.get('type') == 'Pod':
                        if node.get('phase') == 'Pending':
                            pending_workflows += 1
                            break

            self.metrics.get('workflows').get('Pods_pending') \
                .get(worker).set(pending_workflows)


    def check_publish_metrics(self):
        if not self.metrics:
            self.define_metrics()

            if self.metrics:
                self.publish_metrics()
            else:
                error_message = "No workflow workers defined."
                print(error_message, flush=True)
                raise Exception(error_message)

        self.publish_metrics()         

    def __read_worker_sizes(self):
        try:
            workflow_nodes = self.__get_crds("workflowworkers")

            if workflow_nodes.items: 
                for i in workflow_nodes['items']: 
                    pass

            return workflow_nodes
        except client.ApiException as e:
            print(f"Error fetching Argo Workflows: {e}", flush=True) 
            return []


    def __get_crds(self, plural):
        try:
            crd_client = CustomObjectsApi(self.api_client)
            crds = crd_client.list_cluster_custom_object(
                self.group, 
                self.v, 
                plural
            )

            return crds
        except client.ApiException as e:
            print(f"Error fetching CRDS: {e}", flush=True) 
            return []
        
    def label_workflow_nodes(self):
        nodes = sorted([ 
                node for node in self.core_client.list_node().items if filter_nodes_by_label(node.metadata.labels, r"nebulouscloud\.eu/?.+worker?.+") and not node.spec.unschedulable
            ], 
            key=lambda x: (x.status.capacity.get("cpu"), x.status.capacity.get("memory")),
            reverse=True,
        )

        workflow_nodes = sorted(
            self.__get_crds("workflowworkers").get("items"),
            key=lambda x: (x.get("spec").get("cpu"), x.get("spec").get("memory")),
            reverse=True,
        )

        workers = {}

        for node in nodes:
            for workflow_node in workflow_nodes:
                if node.status.capacity.get('cpu') >= workflow_node.get('spec').get('cpu') and \
                    parse_memory_to_bytes(node.status.capacity.get('memory')) >= parse_memory_to_bytes(workflow_node.get('spec').get('memory')):
                        try:
                            body = {
                                "metadata": {
                                    "labels": {
                                        "workflow.nebulouscloud.eu/workersize": workflow_node.get('metadata').get('name')
                                    }
                                }
                            }

                            self.core_client.patch_node(
                                node.metadata.name,
                                body,
                            )
                            if workflow_node.get('metadata').get('name') in workers:
                                workers[workflow_node.get('metadata').get('name')] += 1
                            else:
                                workers[workflow_node.get('metadata').get('name')] = 1
                            break

                        except Exception as e:
                            print(e, flush=True)

        self.workers = workers


    def sync_workflow_nodes(self):
        group = "workflow.io"
        plural = "workflownodes"
        v = "v1"

        nodes = self.core_client.list_node()

        cluster_nodes = {}

        if nodes.items:
            for i in nodes.items:
                if bool(re.match(r".+-ip-.+", i.metadata.name)) and not i.spec.unschedulable:
                    cluster_nodes[i.metadata.name] = i.status.capacity

        crd_client = CustomObjectsApi(self.api_client)
        workflow_nodes = crd_client.list_cluster_custom_object(
            group, 
            v, 
            plural
        )

        worker_nodes = []
        if workflow_nodes.items: 
            for i in workflow_nodes['items']: 
                worker_nodes.append(i['metadata']['name'])

        nodes_to_delete = set(worker_nodes) - set(cluster_nodes.keys())
        nodes_to_add = set(cluster_nodes.keys()) - set(worker_nodes)

        if nodes_to_delete:
            for i in nodes_to_delete: 
                api_response = crd_client.delete_cluster_custom_object(
                    group, 
                    v, 
                    plural,
                    i,
                    body=client.V1DeleteOptions(),
                )
                print(api_response, flush=True)

        if nodes_to_add:
            for i in nodes_to_add: 
                body = {
                    "apiVersion": f"{group}/{v}",
                    "kind": "WorkflowNodes",
                    "metadata": {
                        "name": i
                    },
                    "spec": cluster_nodes[i]
                }

                api_response = crd_client.create_cluster_custom_object(
                    group, 
                    v, 
                    plural,
                    body=body
                )

        self.publish_metrics()

    def get_status():
        pass

    def schedule_job():
        pass

    def schedule_workflow(self, workflow):
        try:

            resources = []

            for template in workflow.get('workflow').get('spec').get('templates'):
                if template.get('script'):
                    if template.get('script').get('resources'):
                        for resource in template.get('script').get('resources').values():
                            resources.append(resource)
                if template.get('container'):
                    if template.get('container').get('resources'):
                        for resource in template.get('container').get('resources').values():
                            resources.append(resource)

            resources = sorted(
                resources, 
                key=lambda x: (x.get('cpu'), parse_memory_to_bytes(x.get('memory'))),
                reverse=False,
            )[-1]

            workflow_nodes = sorted(
                self.__get_crds("workflowworkers").get("items"),
                key=lambda x: (x.get("spec").get("cpu"), x.get("spec").get("memory")),
                reverse=False,
            )

            for workflow_node in workflow_nodes:
                if workflow_node.get('spec').get('cpu') >= resources.get('cpu') and \
                    parse_memory_to_bytes(workflow_node.get('spec').get('memory')) >= parse_memory_to_bytes(resources.get('memory')):
                        if 'labels' not in workflow.get('workflow').get('metadata'):
                            workflow.get('workflow').get('metadata')['labels'] =  {}

                        workflow.get('workflow').get('metadata') \
                            .get('labels')['workflow.nebulouscloud.eu/workersize'] = workflow_node.get("metadata").get("name")
                        for template in workflow.get('workflow').get('spec').get('templates'):
                            template['affinity'] = {
                                'podAffinity': {
                                    'requiredDuringSchedulingIgnoredDuringExecution': [{
                                        'labelSelector': {
                                            'matchLabels': {
                                                'workflow': workflow.get("workflow").get('metadata').get('labels').get('workflow')
                                            },
                                        },
                                        'topologyKey': 'kubernetes.io/hostname',
                                    }]
                                }
                            }
                            template['nodeSelector'] = {
                                'workflow.nebulouscloud.eu/workersize': workflow_node.get("metadata").get("name")
                            }
                        break

            return workflow
        
        except Exception as e:
            error_msg = traceback.format_exc()
            print(error_msg, flush=True)
            return workflow

def main():
    sched = Scheduler()

if __name__ == "__main__":
    main()

    