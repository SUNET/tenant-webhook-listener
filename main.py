import os
import subprocess
import sys
import time
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from queue import Queue, Empty
from threading import Thread
from typing import Optional, Dict

import yaml
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, ValidationError, NaiveDatetime


# define pydantic basemodel for jobs
class Job(BaseModel):
    # allow arbitraty classes to support Queue
    class Config:
        arbitrary_types_allowed = True
        json_encoders = {Queue: lambda v: "Queue"}
    push: Optional[str] = None  # can be target hostname or "all"
    push_lasttime: NaiveDatetime = datetime(1970,1,1)
    push_queued: bool = False
    push_minimum_interval: int = 0
    lasttime: NaiveDatetime = datetime.now()
    enabled: bool = True
    schedule_enabled: bool = False
    run_on_start: bool = False


# define pydantic basemodel for tenants
class Tenant(BaseModel):
    listen_apikey: str
    env_vars: Dict[str, str]
    jobs: Dict[str, Job]


class Tenants(BaseModel):
    tenants: Dict[str, Tenant]


class Trigger(BaseModel):
    tenant_name: str
    job_name: str
    hostname: str
    trigger_source: str = "timer"  # timer, push


class WebhookPost(BaseModel):
    tenant_name: str
    job_name: str
    hostname: str


tenants_parsed: Optional[Tenants] = None


def get_tenants():
    global tenants_parsed
    if tenants_parsed is not None:
        return tenants_parsed

    with open("tenants.yml", "r") as f:
        tenants_parsed = Tenants(**yaml.safe_load(f))

    logger.info("Configured jobs in tenants.yml:")
    for tenant_name, tenant in tenants_parsed.tenants.items():
        for job_name, job in tenant.jobs.items():
            if job.run_on_start:
                # set lasttime to 1970 to force first run via fallback timer
                tenants_parsed.tenants[tenant_name].jobs[job_name].lasttime = datetime(1970, 1, 1)
            else:
                tenants_parsed.tenants[tenant_name].jobs[job_name].lasttime = datetime.now()
            logger.info(f"{tenant_name} {job_name}: {'enabled' if job.enabled else 'disabled'}")
            logger.debug(job)

    return tenants_parsed


def execute_task(all_tenants: Tenants, trigger: Trigger):
    tenant = all_tenants.tenants[trigger.tenant_name]
    job = tenant.jobs[trigger.job_name]
    if not job.enabled:
        return

    if trigger.trigger_source == "push":
        # if push event and minimum interval has passed, execute
        if (datetime.now() - job.push_lasttime) > timedelta(seconds=job.push_minimum_interval):
            # race if another push triggers before timer
            if job.push_queued and job.push != trigger.hostname:
                trigger.hostname = "all"
                all_tenants.tenants[trigger.tenant_name].jobs[trigger.job_name].push_queued = False
            logger.info(f"{datetime.now()} Execute from push: {trigger}")
            all_tenants.tenants[trigger.tenant_name].jobs[trigger.job_name].push_lasttime = datetime.now()
        # if push event but minimum intercval has not passed, wait for timer
        else:
            if job.push_queued:
                # if another push event is already queued with a different hostname, queue "all" instead of maintaining list of many hostnames
                if trigger.hostname != job.push and job.push != "all":
                    all_tenants.tenants[trigger.tenant_name].jobs[trigger.job_name].push = "all"
            else:
                all_tenants.tenants[trigger.tenant_name].jobs[trigger.job_name].push_queued = True
                all_tenants.tenants[trigger.tenant_name].jobs[trigger.job_name].push = trigger.hostname

            logger.debug(f"Delay execute of: {trigger}")
            return
    # catch push events that got delayed trigger because of minimum interval
    elif trigger.trigger_source == "timer" and job.push_queued and (datetime.now() - job.push_lasttime) > timedelta(seconds=job.push_minimum_interval):
        trigger.hostname = job.push
        logger.info(f"{datetime.now()} Execute from push, with delay: {trigger}")
        all_tenants.tenants[trigger.tenant_name].jobs[trigger.job_name].push_lasttime = datetime.now()
        all_tenants.tenants[trigger.tenant_name].jobs[trigger.job_name].push_queued = False
    # Trigger at exact minute after whole hour, if ~1h has passed since last time
    elif trigger.trigger_source == "timer" and job.schedule_enabled and (datetime.now() - job.lasttime) > timedelta(hours=1, seconds=-60) and \
            datetime.now().minute == list(all_tenants.tenants.keys()).index(trigger.tenant_name):
        logger.info(f"{datetime.now()} Execute from timer, exact minute: {trigger}")
        all_tenants.tenants[trigger.tenant_name].jobs[trigger.job_name].lasttime = datetime.now()
    # If above trigger didn't happen, after 2h execute as fallback anyway. This is to prevent jobs from getting stuck if something goes wrong.
    elif trigger.trigger_source == "timer" and (datetime.now() - job.lasttime) > timedelta(hours=2):
        logger.info(f"{datetime.now()} Execute from timer, fallback: {trigger}")
        all_tenants.tenants[trigger.tenant_name].jobs[trigger.job_name].lasttime = datetime.now()
    else:
        return

    try:
        # Popen execute job(hostname=trigger.hostname)
        logger.debug(f"exec {trigger.job_name} with args --hostname={trigger.hostname}")
        newenv = {
            "PATH": f"{os.environ['PATH']}:/opt/tenant-webhook-listener/bin/"
        }
        for env_name, env_value in tenant.env_vars.items():
            newenv[env_name] = env_value
        # check if job is a scriptherder job
        cmd = f"{trigger.job_name} --hostname={trigger.hostname}"
        # execute job
        subprocess.run(cmd, shell=True, check=True, env={**os.environ, **newenv})
    except subprocess.CalledProcessError as e:
        logger.warning(f"{datetime.now()} {trigger.job_name} for {trigger.tenant_name} failed with exit code {e.returncode}")
        # TODO: send notification
    except Exception as e:
        logger.error(f"{datetime.now()} {trigger.job_name} for {trigger.tenant_name} failed with exception {e}")
        # TODO: send notification
    finally:
        pass
        # update last run time
        #all_tenants.tenants[trigger.tenant_name].jobs[trigger.job_name].push_lasttime = datetime.now()


def scheduler_thread(q: Queue):
    try:
        all_tenants = get_tenants()
    except ValidationError as e:
        print(e)
        sys.exit(1)

    while True:
        try:
            trigger: Trigger = q.get(timeout=1)
            assert isinstance(trigger, Trigger)
        except Empty:
            trigger: Optional[Trigger] = None
        except AssertionError:
            print("Invalid trigger received")
            time.sleep(1)
            continue

        if trigger:
            if trigger.trigger_source == "terminate":
                print("Terminate thread")
                break
            execute_task(all_tenants, trigger)
        else:
            for tenant_name, tenant in all_tenants.tenants.items():
                for job_name, job in tenant.jobs.items():
                    trigger = Trigger(tenant_name=tenant_name, job_name=job_name, hostname="all", trigger_source="timer")
                    execute_task(all_tenants, trigger)

        time.sleep(0.1)  # max 10 jobs per second if queue is full


scheduler_queue = Queue()


@asynccontextmanager
async def lifespan(app: FastAPI):
    t1 = Thread(target=scheduler_thread, args=(scheduler_queue,))
    t1.daemon = True
    t1.start()
    yield
    scheduler_queue.put(Trigger(tenant_name="terminate", job_name="terminate", hostname="terminate", trigger_source="terminate"))
    t1.join(timeout=10)

app = FastAPI(lifespan=lifespan)
logger = logging.getLogger('uvicorn.error')

header_scheme = APIKeyHeader(name="X-API-Key")

@app.post("/webhook/")
async def webhook(item: WebhookPost, key: str = Depends(header_scheme)):
    target_tenant: Optional[str] = None
    all_tenants = get_tenants()

    for tenant_name, tenant in all_tenants.tenants.items():
        if tenant.listen_apikey is not None and tenant.listen_apikey == key:
            target_tenant = tenant_name
            break
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid API key"
        )

    if item.tenant_name != target_tenant:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Mismatch between tenant_name in POST data and apikey tenant"
        )

    scheduler_queue.put(Trigger(tenant_name=target_tenant, job_name=item.job_name, hostname=item.hostname, trigger_source="push"))
    # return ok status
    return {"status": "ok"}

