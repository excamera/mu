/* -*-mode:c++; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include <Python.h>
#include <iostream>
#include <string>
#include <vector>

#include "launch.hh"

using namespace std;

static PyObject *pylaunch_launchpar(PyObject *self, PyObject *args);

// method table and initialization
static PyMethodDef pylaunch_Methods[] = {
    {"launchpar", pylaunch_launchpar, METH_VARARGS, "Launch many lambdas in parallel."},
    {NULL, NULL, 0, NULL }
};
PyMODINIT_FUNC initpylaunch(void) {
    cerr << "Initializing pylaunch... ";
    (void) Py_InitModule("pylaunch", pylaunch_Methods);
    cerr << "done." << endl;
}

// call launchpar from python
static PyObject *pylaunch_launchpar(PyObject *self __attribute__((unused)), PyObject *args) {
    int nlaunch;
    char *fn_name, *akid, *secret, *payload;
    PyObject *lambda_regions_obj;
    if (! PyArg_ParseTuple(args, "issssO!", &nlaunch, &fn_name, &akid, &secret, &payload, &PyList_Type, &lambda_regions_obj)) {
        return NULL;
    }

    vector<string> lambda_regions;
    int nregions = PyList_Size(lambda_regions_obj);
    for (int i = 0; i < nregions; i++) {
        PyObject *region = PyList_GetItem(lambda_regions_obj, i);
        lambda_regions.emplace_back(string(PyString_AsString(region)));
    }

    launchpar(nlaunch, string(fn_name), string(akid), string(secret), string(payload), lambda_regions);

    Py_RETURN_NONE;
}
