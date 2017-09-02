from fabric.api import cd, env, run, parallel, task, roles, put, shell_env, remote_tunnel, settings
from fabric.contrib.files import exists
from StringIO import StringIO

env.use_ssh_config = True
env.hosts = ['monitor01', 'monitor02', 'monitor03', 'monitor04']

MASTER = env.hosts[0]
SLAVES = env.hosts[1:]

env.roledefs = {
    'master': [MASTER],
    'slave': SLAVES,
}

MASTER_WEBUI_PORT = 8101


@task
@parallel
def info():
    run('egrep "model name" /proc/cpuinfo')
    run('egrep "MemTotal|MemFree" /proc/meminfo')


@task
@roles('master')
def venv():
    if not exists('.venv'):
        run('virtualenv --python=python3 .venv')
    run('.venv/bin/pip install numpy scipy matplotlib ipython jupyter pandas sympy nose plotly')
    run('.venv/bin/pip install geojson descartes')


@task
@parallel
@roles('slave')
def ping():
    run('ping -c 10 %s' % MASTER)


@task
@roles('master')
def cluster(task='start'):
    assert task in ('start', 'stop')
    with settings(warn_only=True):
        run('./spark/sbin/%s-master.sh' % task)
        run('./spark/sbin/%s-slaves.sh' % task)


@task
@roles('master')
def config(settings='default', slaves=3):
    slaves = int(slaves)
    assert settings in ('default', 'multiple')
    assert slaves in range(1, 3)

    put(StringIO(('\n'.join(SLAVES[:slaves])) + '\n'), 'spark/conf/slaves')

    # https://issues.apache.org/jira/browse/SPARK-6313
    put(StringIO('spark.files.useFetchCache false\n'), 'spark/conf/spark-defaults.conf')

    spark_env = '''\
#!/usr/bin/env bash
export SPARK_LOCAL_DIRS=${{SPARK_HOME}}/tmp
export SPARK_MASTER_WEBUI_PORT={master_webui_port}
export PYTHONHASHSEED=1
export PYSPARK_PYTHON=~/.venv/bin/python3
export SPARK_MASTER_PORT=8888
export SPARK_MASTER_HOST=192.100.100.156
export SPARK_HOME=/usr/lab/alum/0077162/spark
'''.format(master_webui_port=MASTER_WEBUI_PORT)
    if settings == 'multiple':
        spark_env += '''\
export SPARK_WORKER_INSTANCES=2
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=5g  # Servers have 16GB but it is a shared environment, let's be safe
'''
    else:
        spark_env += '''\
export SPARK_WORKER_MEMORY=10g  # Servers have 16GB but it is a shared environment, let's be safe
'''
    put(StringIO(spark_env), 'spark/conf/spark-env.sh')


@task
@roles('master')
def pyspark():
    # with remote_tunnel(MASTER_WEBUI_PORT):
        with remote_tunnel(4040):  # SparkContext
            with shell_env(
                    PYTHONHASHSEED='1',
                    PYSPARK_PYTHON='python3',
                    PYSPARK_DRIVER_PYTHON='ipython'):
                with cd('pfc'):
                    run('~/spark/bin/pyspark --master local[4]')


@task
@roles('master')
def notebook():
    with remote_tunnel(8888):
        with shell_env(
                PYTHONHASHSEED='1',
                PYSPARK_PYTHON='python3',
                PYSPARK_DRIVER_PYTHON='jupyter',
                PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser"):
            run('./spark/bin/pyspark --master local[4]')
