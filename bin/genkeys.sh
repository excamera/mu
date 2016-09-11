#!/bin/bash

# generate all the keys
for i in ca server server2; do
    openssl genrsa -out ${i}_key.pem 2048
done

# CA key is self-signed
openssl req -x509 -new -sha256 -nodes -key ca_key.pem -days 3650 -out ca_cert.pem -subj "/C=US/ST=CA/O=mu/CN=mu_ca"

# signing request
for i in server server2; do
    openssl req -new -sha256 -key ${i}_key.pem -out ${i}_csr.pem -subj "/C=US/ST=NY/O=mu/CN=mu_${i}"
done

# sign
for i in server server2; do
    openssl x509 -req -in ${i}_csr.pem -CA ca_cert.pem -CAkey ca_key.pem -CAcreateserial -out ${i}_cert.pem -days 3650
done

rm ca_cert.srl server_csr.pem server2_csr.pem

# now do something like
# openssl s_server -CAfile server_chain.pem -cert server_chain.pem -key server_key.pem -debug
