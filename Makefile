
develop: 
	pip install .

clean:
	rm -rf dist/
	rm -rf build/
	rm -rf ybmetrics.egg-info/

build: clean
	python3 -m build


.PHONY: clean build install develop
