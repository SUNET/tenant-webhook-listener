import sys
import time

from fastapi import FastAPI
from queue import Queue, Empty
from pydantic import BaseModel, ValidationError, NaiveDatetime
from typing import Optional, Dict
from datetime import datetime, timedelta
from threading import Thread
from contextlib import asynccontextmanager

debug = True

tenants_dict = {
    "tenants": {
        "konstfack": {
            "listen_apikey": "random123",
            "netbox_name": "Konstfack",
            "netbox_api_token": "random456",
            "jobs": {
                "netbox_devices": {
                    "enabled": True,
                    "push_minimum_interval": 5,
                    "push": None,
                    "push_queued": False,
                    "push_lasttime": datetime(1970,1,1),
                    "lasttime": datetime.now(),
                    "queue": Queue(),
                }
            }
        }
    }
}


# define pydantic basemodel for jobs
class Job(BaseModel):
    # allow arbitraty classes to support Queue
    class Config:
        arbitrary_types_allowed = True
        json_encoders = {Queue: lambda v: "Queue"}
    push: Optional[str] = None  # can be target hostname or "all"
    push_lasttime: NaiveDatetime
    push_queued: bool = False
    push_minimum_interval: int = 0
    lasttime: NaiveDatetime
    queue: Queue
    enabled: bool


# define pydantic basemodel for tenants
class Tenant(BaseModel):
    listen_apikey: str
    netbox_name: str
    netbox_api_token: str
    jobs: Dict[str, Job]


class Tenants(BaseModel):
    tenants: Dict[str, Tenant]


class Trigger(BaseModel):
    tenant_name: str
    job_name: str
    hostname: str
    trigger_source: str = "timer"  # timer, push


class WebhookPost(BaseModel):
    job_name: str
    hostname: str


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
            if debug:
                print(f"{datetime.now()} Execute from push: {trigger}")
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

            if debug:
                print(f"Delay execute of: {trigger}")
            return
    # catch push events that got delayed trigger because of minimum interval
    elif trigger.trigger_source == "timer" and job.push_queued and (datetime.now() - job.push_lasttime) > timedelta(seconds=job.push_minimum_interval):
        trigger.hostname = job.push
        if debug:
            print(f"{datetime.now()} Execute from push, with delay: {trigger}")
        all_tenants.tenants[trigger.tenant_name].jobs[trigger.job_name].push_lasttime = datetime.now()
        all_tenants.tenants[trigger.tenant_name].jobs[trigger.job_name].push_queued = False
    # Trigger at exact minute after whole hour, if ~1h has passed since last time
    elif trigger.trigger_source == "timer" and (datetime.now() - job.lasttime) > timedelta(hours=1, seconds=-60) and \
            datetime.now().minute == list(all_tenants.tenants.keys()).index(trigger.tenant_name):
        if debug:
            print(f"{datetime.now()} Execute from timer, exact minute: {trigger}")
        all_tenants.tenants[trigger.tenant_name].jobs[trigger.job_name].lasttime = datetime.now()
    # If above trigger didn't happen, after 2h execute as fallback anyway. This is to prevent jobs from getting stuck if something goes wrong.
    elif trigger.trigger_source == "timer" and (datetime.now() - job.lasttime) > timedelta(hours=2):
        if debug:
            print(f"{datetime.now()} Execute from timer, fallback: {trigger}")
        all_tenants.tenants[trigger.tenant_name].jobs[trigger.job_name].lasttime = datetime.now()
    else:
        return

    try:
        # Popen execute job(hostname=trigger.hostname)
        print(f"exec {trigger.job_name} with args --hostname={trigger.hostname}")
        pass
    finally:
        pass
        # update last run time
        #all_tenants.tenants[trigger.tenant_name].jobs[trigger.job_name].push_lasttime = datetime.now()


def scheduler_thread(q: Queue):
    try:
        all_tenants = Tenants(**tenants_dict)
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

@app.post("/webhook/")
async def webhook(item: WebhookPost):
    tenant_name = "konstfack"
    scheduler_queue.put(Trigger(tenant_name=tenant_name, job_name=item.job_name, hostname=item.hostname, trigger_source="push"))

