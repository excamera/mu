clean-local:
	@rm -f pylaunch.so
	@find . -type f -name '*.pyc' -delete

all-local: ../pylaunch/pylaunch.la
	@cp ../pylaunch/.libs/pylaunch.so .

EXTRA_DIST = .gitignore .pylintrc Makefile.am README_xc-enc.md lambda_extra_packages.tar.gz lambda_function_template.py lambda_state_server.py lambdaize.sh libmu/__init__.py libmu/defs.py libmu/fd_wrapper.py libmu/handler.py libmu/machine_state.py libmu/server.py libmu/socket_nb.py libmu/util.py png2y4m_server.py test/__init__.py test/__main__.py test/client_test.py test/defs.py test/encsrv.py test/run.py test/server_test.py test/states.py test/util.py vpx_ssim_server.py vpxenc_server.py xcenc_server.py
