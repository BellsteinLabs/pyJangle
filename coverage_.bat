@ECHO OFF
ECHO TEST
coverage run -m unittest discover -v -s .\tests -p *_test.py
coverage xml -o coverage.xml