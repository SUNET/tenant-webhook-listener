# Tenant webhook listener

Run scripts for different tenants (with different env vars set) based on schedule or triggered via API calls.

## Installation

Set up a new venv in for ex /opt/tenant-webhook-listener/venv/ and install requirements.txt:

```
cd /opt/tenant-webhook-listener
python3 -m venv venv
cd venv
source bin/activate
pip install -r /path/to/requirements.txt
```

Copy example systemd service to for ex /etc/systemd/system/webhook-uvicorn.service and start it

Use together with nginx config:

```
upstream fastapi {
    server unix:/run/uvicorn/webhook.socket;
}

server {
    ...
    
    location /api/ {
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Host $http_host;
        proxy_redirect off;
        proxy_pass http://fastapi/;
    }
}
```

## Configure

Configure tenants.yml with scripts to run and env vars:

```
tenants:
  tenant1:
    jobs:
      devices_job:
        enabled: true
        schedule_enabled: true
        push: null
        push_minimum_interval: 5
        push_queued: false
    listen_apikey: longrandomstring
    env_vars:
      BASE_URL: "https://xyz"
      API_TOKEN: key
```

This will run executable "devices_job" from PATH or /opt/tenant-webhook-listener/bin/ every hour or when triggered like so:

```
curl -s -X POST -d '{"tenant_name": "tenant1", "job_name": "devices_job", "hostname": "myhost"}' -H "X-API-Key: longrandomstring" -H "Content-type: application/json" https://webhook.sunet.se/api/webhook/ | jq .
```
