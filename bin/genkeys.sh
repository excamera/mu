#!/bin/bash

# generate all the keys
for i in ca server; do
    openssl genrsa -out ${i}_key.pem 2048
done

# CA key is self-signed
openssl req -x509 -new -sha256 -nodes -key ca_key.pem -days 3650 -out ca_cert.pem -subj "/C=US/ST=CA/O=mu/CN=mu_ca"

# signing request
openssl req -new -sha256 -key server_key.pem -out server_csr.pem -subj "/C=US/ST=NY/O=mu/CN=mu_srv"

# sign
openssl x509 -req -in server_csr.pem -CA ca_cert.pem -CAkey ca_key.pem -CAcreateserial -out server_cert.pem -days 3650

rm ca_cert.srl server_csr.pem

# now do something like
# openssl s_server -CAfile server_chain.pem -cert server_chain.pem -key server_key.pem -debug
