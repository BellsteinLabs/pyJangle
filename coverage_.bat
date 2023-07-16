@ECHO OFF
ECHO TEST
coverage run -m unittest discover -v -s ./ -p *_test.py
coverage xml -o coverage.xml