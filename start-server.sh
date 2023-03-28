# Clear file contents.
> ./cluster-server.env

# Initialize service address.
echo "NODE='${1}'" >> ./cluster-server.env

python3 server.py