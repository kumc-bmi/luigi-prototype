# luigi-prototype
This is a Vagrant box and Ansible provisioning playbook that I generated while building a small SQL-based ETL Luigi prototype. Below are instructions to get the Vagrant box up and running.
## Installation
### Step 1. Required Software
Make sure that following are installed locally:
 * [VirtualBox]
 * [Vagrant]
 * [Ansible]

### Step 2. Pull Down Repo
Clone this repository somewhere on your local machine.
```sh
$ cd <somewhere>
$ git clone https://github.com/mprittie/luigi-prototype.git
```
### Step 3. Bring Up Vagrant Box
To bring up the VM run the following command from within the `<somewhere>` directory (which shoudl contain a `VagrantFile`).  I may take a few minutes for the Vagrant to download the base VM image (`ubuntu/trusty64`) and run the Ansible playbook which provisions the VM.
```sh
$ vagrant up
```

### Step 4: Luigi UI
If the the Ansible playbook ran successfully you should be able to pull up the Luigi UI on http://localhost:8082.

### Step 5: Login to VM and Initialize
**Todo:** This should be added to the Ansible playbook.

Log into the VM by running the following command:
```sh
$ vagrant ssh
```
Once you have a SSH connection to the VM run the following to initialize some source tables in PostgreSQL.
```sh
$ psql -U luigi -d pcornet -f /vagrant/PCORNetLoader/sql/init.sql
```
**Note:** The password for the `luigi` PostgreSQL user is `password`.

## Simple ETL Workflow
The code for this workflow can be found in `PCORNetLoader/pcornet/tasks.py`.  This workflow does not actually do anything and is simply a skeletal framework with `sleep()` executed within each task.  To run this workflow run the following commands from within the VM:
```sh
$ cd /vagrant/PCORNetLoader
$ source ../venv/bin/activate
(venv)$ python -m luigi --module pcornet.tasks PCORnetETL --workers=2
```
You should be able to watch the tasks complete by viewing the dependancy graph in the Luigi UI.

## PostgreSQL ETL Workflow
The code for this workflow can be found in `PCORNetLoader/pcornet/tasks2.py`.  This This workflow actually does a small ETL from the source tables into destination tables using intermediary transform views.  To run this workflow run the following commands:
```sh
(venv)$ python -m luigi --module pcornet.tasks2 PCORnetETL --workers=2
```

[VirtualBox]: <https://www.virtualbox.org/wiki/Downloads>
[Vagrant]: <https://www.vagrantup.com/downloads.html>
[Ansible]:  <https://git-scm.com/downloads>
