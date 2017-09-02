zip -r lib.zip lib -x *.pyc;PYTHONHASHSEED=1 PYSPARK_PYTHON=~/.venv/bin/python3 PYSPARK_DRIVER_PYTHON=~/.venv/bin/ipython .././spark/bin/pyspark --master spark://monitor01:7077 --py-files lib.zip
