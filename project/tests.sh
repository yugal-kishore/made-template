#!/bin/bash 

if ! command -v python3 >/dev/null 2>&1
then echo"Pipeline needs python 3 but it's not installed"
    exit
fi

python3 -m pip install -r ./project/requirements.txt



echo "pipeline running"
python3 ./project/pipeline.py


echo "test cases running"
pytest ./project/tests.py

exit