clean-local:
	@rm -f pylaunch.so
	@find . -type f -name '*.pyc' -delete

all-local: ../pylaunch/pylaunch.la
	@cp ../pylaunch/.libs/pylaunch.so .

EXTRA_DIST = .pylintrc Makefile.am lambda_extra_packages.tar.gz lambda_function_template.py lambdaize.sh libmu/__init__.py libmu/defs.py libmu/fd_wrapper.py libmu/handler.py libmu/machine_state.py libmu/server.py libmu/socket_nb.py libmu/util.py test/__init__.py test/__main__.py test/data/server_chain.pem test/data/server_key.pem test/defs.py test/encsrv.py test/run.py test/states.py test/util.py test/xcenc_client_test.py test/xcenc_server_test.py vpxenc_server.py xcenc_server.py
