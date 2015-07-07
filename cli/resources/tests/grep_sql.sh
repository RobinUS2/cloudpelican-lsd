cloudpelican -v -e 'cat errors | grep -v 404 | grep -i checkout | grep -e "(100|200)" | grep -v -i -e ''404'''
