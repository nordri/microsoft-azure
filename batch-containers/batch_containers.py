from __future__ import print_function
import datetime
import io
import os
import sys
import time
import config
import argparse
try:
    input = raw_input
except NameError:
    pass

import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batch_auth
import azure.batch.models as batchmodels

sys.path.append('.')
sys.path.append('..')

# Based on https://github.com/Azure-Samples/batch-python-quickstart/blob/master/src/python_quickstart_client.py

# Update the Batch, Registry, Docker image and Storage account credential 
# strings in config.py with values unique to your accounts. These are used
# when constructing connection strings for the Batch and Storage client objects.

def query_yes_no(question, default="yes"):
    """
    Prompts the user for yes/no input, displaying the specified question text.

    :param str question: The text of the prompt for input.
    :param str default: The default if the user hits <ENTER>. Acceptable values
    are 'yes', 'no', and None.
    :rtype: str
    :return: 'yes' or 'no'
    """
    valid = {'y': 'yes', 'n': 'no'}
    if default is None:
        prompt = ' [y/n] '
    elif default == 'yes':
        prompt = ' [Y/n] '
    elif default == 'no':
        prompt = ' [y/N] '
    else:
        raise ValueError("Invalid default answer: '{}'".format(default))

    while 1:
        choice = input(question + prompt).lower()
        if default and not choice:
            return default
        try:
            return valid[choice[0]]
        except (KeyError, IndexError):
            print("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")


def print_batch_exception(batch_exception):
    """
    Prints the contents of the specified Batch exception.

    :param batch_exception:
    """
    print('-------------------------------------------')
    print('Exception encountered:')
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        print(batch_exception.error.message.value)
        if batch_exception.error.values:
            print()
            for mesg in batch_exception.error.values:
                print('{}:\t{}'.format(mesg.key, mesg.value))
    print('-------------------------------------------')

def create_pool(batch_service_client, pool_id):
    """
    Creates a pool of compute nodes with the specified OS settings.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str pool_id: An ID for the new pool.
    :param str publisher: Marketplace image publisher
    :param str offer: Marketplace image offer
    :param str sku: Marketplace image sku
    """
    print('Creating pool [{}]...'.format(pool_id))

    # Create a new pool of Linux compute nodes using an Azure Virtual Machines
    # Marketplace image. For more information about creating pools of Linux
    # nodes, see:
    # https://azure.microsoft.com/documentation/articles/batch-linux-nodes/
    # In this case is a pool ready to run Docker containers
    image_ref_to_use = batch.models.ImageReference(
        publisher='microsoft-azure-batch',
        offer='ubuntu-server-container',
        sku='16-04-lts',
        version='latest'
    )

    # Specify a container registry
    # We got the credentials from config.py
    containerRegistry = batchmodels.ContainerRegistry(
        user_name=config._REGISTRY_USER_NAME, 
        password=config._REGISTRY_PASSWORD, 
        registry_server=config._REGISTRY_SERVER
    )

    # The instance will pull the images defined here
    container_conf = batchmodels.ContainerConfiguration(
        container_image_names=[config._DOCKER_IMAGE],
        container_registries=[containerRegistry]
    )

    new_pool = batch.models.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=image_ref_to_use,
            container_configuration=container_conf,
            node_agent_sku_id='batch.node.ubuntu 16.04'),
        vm_size='STANDARD_A2',
        target_dedicated_nodes=1
    )

    batch_service_client.pool.add(new_pool)

def create_job(batch_service_client, job_id, pool_id):
    """
    Creates a job with the specified ID, associated with the specified pool.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID for the job.
    :param str pool_id: The ID for the pool.
    """
    print('Creating job [{}]...'.format(job_id))

    job = batchmodels.JobAddParameter(
        id=job_id,
        pool_info=batchmodels.PoolInformation(pool_id=pool_id))

    batch_service_client.job.add(job)

def add_tasks(batch_service_client, job_id, task_id, number_to_test):
    """
    Adds a task for each input file in the collection to the specified job.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID of the job to which to add the tasks.
     created for each input file.
    :number_to_test: number you want to know if it's prime or not.
    """

    print('Adding tasks to job [{}]...'.format(job_id))

    # This is the user who run the command inside the container.
    # An unprivileged one
    user = batchmodels.AutoUserSpecification(
        scope=batchmodels.AutoUserScope.task,
        elevation_level=batchmodels.ElevationLevel.non_admin
    )

    # This is the docker image we want to run
    task_container_settings = batchmodels.TaskContainerSettings(
        image_name=config._DOCKER_IMAGE,
        container_run_options='--rm'
    )
    
    # The container needs this argument to be executed
    task = batchmodels.TaskAddParameter(
        id=task_id,
        command_line='python /is_prime.py ' + str(number_to_test),
        container_settings=task_container_settings,
        user_identity=batchmodels.UserIdentity(auto_user=user)
    )

    batch_service_client.task.add(job_id, task)

def wait_for_tasks_to_complete(batch_service_client, job_id, timeout):
    """
    Returns when all tasks in the specified job reach the Completed state.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The id of the job whose tasks should be to monitored.
    :param timedelta timeout: The duration to wait for task completion. If all
    tasks in the specified job do not reach Completed state within this time
    period, an exception will be raised.
    """
    timeout_expiration = datetime.datetime.now() + timeout

    print("Monitoring all tasks for 'Completed' state, timeout in {}..."
          .format(timeout), end='')

    while datetime.datetime.now() < timeout_expiration:
        print('.', end='')
        sys.stdout.flush()
        tasks = batch_service_client.task.list(job_id)

        incomplete_tasks = [task for task in tasks if
                            task.state != batchmodels.TaskState.completed]
        if not incomplete_tasks:
            print()
            return True
        else:
            time.sleep(1)

    print()
    raise RuntimeError("ERROR: Tasks did not reach 'Completed' state within "
                       "timeout period of " + str(timeout))


def print_task_output(batch_service_client, job_id, task_id, encoding=None):
    """Prints the stdout.txt and stderr.txt file for the task in the job.

    :param batch_client: The batch client to use.
    :type batch_client: `batchserviceclient.BatchServiceClient`
    :param str job_id: The id of the job with task output files to print.
    :param str task_id: The id of the task with output files to print.
    """
    
    print('Printing task output...')

    print("Task: {}".format(task_id))
    
    stream = batch_service_client.file.get_from_task(job_id, task_id, config._STANDARD_OUT_FILE_NAME)
    file_text = _read_stream_as_string(
        stream,
        encoding)
    print("Standard output:")
    print(file_text)

    stream = batch_service_client.file.get_from_task(job_id, task_id, config._STANDARD_ERR_FILE_NAME)
    file_text = _read_stream_as_string(
        stream,
        encoding)
    print("Standard error:")
    print(file_text)
    
def _read_stream_as_string(stream, encoding):
    """Read stream as string

    :param stream: input stream generator
    :param str encoding: The encoding of the file. The default is utf-8.
    :return: The file content.
    :rtype: str
    """
    output = io.BytesIO()
    try:
        for data in stream:
            output.write(data)
        if encoding is None:
            encoding = 'utf-8'
        return output.getvalue().decode(encoding)
    finally:
        output.close()
    raise RuntimeError('could not write data to stream or decode bytes')

if __name__ == '__main__':

    start_time = datetime.datetime.now().replace(microsecond=0)
    print('Sample start: {}'.format(start_time))
    print()

    parser = argparse.ArgumentParser()
    parser.add_argument("number", help="the number we want to test",
        type=int)
    args = parser.parse_args()

    # Create a Batch service client. We'll now be interacting with the Batch
    # service in addition to Storage
    credentials = batch_auth.SharedKeyCredentials(config._BATCH_ACCOUNT_NAME,
                                                 config._BATCH_ACCOUNT_KEY)
    batch_client = batch.BatchServiceClient(
        credentials,
        base_url=config._BATCH_ACCOUNT_URL)

    try:
        # Create the pool that will contain the compute nodes that will execute the
        # tasks.
        create_pool(batch_client, config._POOL_ID)
        
        # Create the job that will run the tasks.
        create_job(batch_client, config._JOB_ID, config._POOL_ID)

        # Add the tasks to the job. 
        add_tasks(batch_client, config._JOB_ID, config._TASK_ID, args.number)

        # Pause execution until tasks reach Completed state.
        wait_for_tasks_to_complete(batch_client,
                               config._JOB_ID,
                               datetime.timedelta(minutes=30))

        print("  Success! All tasks reached the 'Completed' state within the "
          "specified timeout period.")

        # Print the stdout.txt and stderr.txt files for each task to the console
        print_task_output(batch_client, config._JOB_ID, config._TASK_ID)

    except batchmodels.BatchErrorException as err:
        print_batch_exception(err)
        raise

    # Print out some timing info
    end_time = datetime.datetime.now().replace(microsecond=0)
    print()
    print('Sample end: {}'.format(end_time))
    print('Elapsed time: {}'.format(end_time - start_time))
    print()

    # Clean up Batch resources (if the user so chooses).
    if query_yes_no('Delete job?') == 'yes':
        batch_client.job.delete(config._JOB_ID)

    if query_yes_no('Delete pool?') == 'yes':
        batch_client.pool.delete(config._POOL_ID)

    print()
    input('Press ENTER to exit...')
