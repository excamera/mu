/* -*-mode:c++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include <iostream>
#include <cstdlib>

#include "socket.hh"
#include "secure_socket.hh"
#include "exception.hh"
#include "http_request.hh"
#include "http_response_parser.hh"
#include "lambda_request.hh"
#include "getenv.hh"

using namespace std;

void say_hello(char *sname, char *sport);

int main(int argc, char **argv)
{
  try {
    if (argc > 2) {
        say_hello(argv[1], argv[2]);
    } else {
        say_hello((char *)"www.example.com", (char *)"https");
    }
  } catch ( const exception & e ) {
    print_exception( e );
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}

void say_hello(char *sname, char *sport)
{
  /* open connection to server */
  TCPSocket sock;
  Address server { sname, sport };
  cerr << "Connecting to " << server.str() << "... ";
  sock.connect( server );
  cerr << "done.\n";

  SSLContext ctx;
  cerr << "Setting up SSL connection... ";
  SecureSocket www = ctx.new_secure_socket(move(sock));
  www.connect();
  cerr << "done.\n";

  /* prepare request */
  string fn_name = safe_getenv("LAMBDA_FUNCTION");
  string secret = safe_getenv("AWS_SECRET_ACCESS_KEY");
  string akid = safe_getenv("AWS_ACCESS_KEY_ID");
  LambdaInvocation ll(secret, akid, fn_name);
  //LambdaListFunctions ll(secret, akid);
  //LambdaListFnVersions ll(secret, akid, fn_name);
  HTTPRequest request = ll.to_http_request();

  /* send request */
  cerr << "Sending request:\n";
  cerr << request.str() << '\n';
  www.write( request.str() );
  cerr << "done.\n";

  /* construct response parser */
  HTTPResponseParser parser;

  /* tell parser about the request */
  parser.new_request_arrived( request );

  /* read reply */
  while ( not www.eof() ) {
    parser.parse( www.read() );

    /* did we get a reply? */
    if ( not parser.empty() ) {
      cerr << "Got reply.\n";
      const HTTPResponse & response = parser.front();
      for ( unsigned int i = 0; i < response.headers().size(); i++ ) {
        cout << "header # " << i << ": key={" << response.headers().at( i ).key()
             << "}, value={" << response.headers().at( i ).value() << "}\n";
      }

      cout << "Body (" << response.body().size() << " bytes) follows:\n";
      cout << response.body();
      cout << "\n[end of body]\n";

      break; /* we only sent one request */
    }
  }

  www.close();
}
