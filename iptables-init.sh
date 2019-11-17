#!/bin/sh

# Don't redirect anything for the proxy process itself to avoid endless loop.
# Also, avoid the proxy looping back to itself if someone tries a direct a direct
# connection to the proxy port.
iptables -t nat -A OUTPUT -p tcp --dport 27017 -m owner --uid-owner 9999 -j RETURN
iptables -t filter -A OUTPUT -p tcp --dport 27111 -m owner --uid-owner 9999 -j REJECT

# Send all MongoDb traffic to the proxy sidecar
iptables -t nat -A OUTPUT -p tcp --dport 27017 -j REDIRECT --to-port 27111
