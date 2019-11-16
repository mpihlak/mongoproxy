#!/bin/sh

# Don't redirect anything for the proxy process itself to avoid endless loop
iptables -t nat -A OUTPUT -p tcp --dport 27017 -m owner --uid-owner 9999 -j RETURN

# Send all MongoDb traffic to the proxy sidecar
iptables -t nat -A OUTPUT -p tcp --dport 27017 -j REDIRECT --to-port 27111
