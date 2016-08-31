/* -*-mode:c++; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include <iostream>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <vector>
#include <unistd.h>

#include "exception.hh"
#include "getenv.hh"
#include "launch.hh"

using namespace std;

void json_from_env(const char *varname, stringstream &strm, char pre_char, bool throw_if_empty, const char *dflt);
void read_cert_file(const char *varname, stringstream &strm);
vector<string> get_lambda_regions(void);
string build_payload(void);

/*
static const vector<string> lambda_regions = { {"us-east-1"}        // Oregon
                                             , {"us-west-2"}        // Virginia
                                             , {"eu-west-1"}        // Ireland
                                             , {"eu-central-1"}     // Frankfurt
                                             , {"ap-northeast-1"}   // Tokyo
                                             , {"ap-southeast-2"}   // Sydney
                                             };
*/

int main(int argc, char **argv)
{
    int nlaunch = 0;
    if (argc > 1) {
        nlaunch = atoi(argv[1]);
    }
    if (nlaunch < 1) {
        nlaunch = 1;
    }

    try {
        string fn_name = safe_getenv("LAMBDA_FUNCTION");
        string akid = safe_getenv("AWS_ACCESS_KEY_ID");
        string secret = safe_getenv("AWS_SECRET_ACCESS_KEY");

        vector<string> lambda_regions = get_lambda_regions();
        string payload = build_payload();

        launchpar(nlaunch, fn_name, akid, secret, payload, lambda_regions);
    } catch ( const exception & e ) {
        print_exception( e );
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

void json_from_env(const char *varname, stringstream &strm, char prechar, bool throw_if_empty, const char *dflt) {
    string envvarname("EVENT_");
    envvarname += varname;
    char *value = getenv(envvarname.c_str());

    if (!value) {
        if (throw_if_empty) {
            string err = "missing required environment variable: EVENT_";
            err += varname;
            throw runtime_error(err);
        } else if (dflt == NULL) {
            return;
        }

        value = (char *)dflt;
    }

    strm << prechar << '"' << varname << "\":\"" << value << '"';
}

void read_cert_file(const char *varname, stringstream &strm) {
    string envvarname("EVENT_");
    envvarname += varname;
    char *filename = getenv(envvarname.c_str());

    if (!filename) {
        return;
    }

    ifstream cert(filename);
    if (!cert.good()) {
        return;
    }

    string tmp;
    strm << ',' << '"' << varname << "\":\"";

    bool started = false;
    while (! cert.eof()) {
        getline(cert, tmp);
        if (tmp.find("-----BEGIN ") == 0) {
            started = true;
            continue;
        } else if (tmp.find("-----END ") == 0 || tmp == "") {
            break;
        }

        if (started) {
            strm << tmp;
        }
    }

    strm << '"';
}

vector<string> get_lambda_regions(void) {
    vector<string> lambda_regions;
    {
        stringstream rstrm(safe_getenv("LAMBDA_REGIONS"));
        string tmp;
        while (! rstrm.eof()) {
            getline(rstrm, tmp, ',');
            lambda_regions.push_back(tmp);
        }
    }

    return lambda_regions;
}

string build_payload(void) {
    stringstream pstream;

    // required  parts
    json_from_env("addr", pstream, '{', true, NULL);
    json_from_env("port", pstream, ',', true, NULL);

    // optional parts
    read_cert_file("cacert", pstream);
    read_cert_file("srvcrt", pstream);
    read_cert_file("srvkey", pstream);
    json_from_env("mode", pstream, ',', false, "1");

    json_from_env("bucket", pstream, ',', false, NULL);
    json_from_env("nonblock", pstream, ',', false, NULL);
    json_from_env("expect_statefile", pstream, ',', false, NULL);
    json_from_env("rm_tmpdir", pstream, ',', false, NULL);

    pstream << '}';

    return pstream.str();
}
