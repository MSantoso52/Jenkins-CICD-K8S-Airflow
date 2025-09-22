# Jenkins-CICD-K8S-Airflow
Automate CI/CD Pipeline using Jenkin to Airflow on Kubernetes
![Picture of a workflow](jenkins.png)
# *Overview*
Automate CI/CD Pipeline Airflow DAG using Jenkins, Airflow DAG is tested everytime build before deploy into Airflow on Kubernetes. The workflow using Github repository, using push event to trigger Jenkins to start build process, the code start load --> test --> deploy. Only pass test will deploy into production, if fail continue to development phase.  
# *Prerequisites*
To follow along this project need to be available & ready on system:
1. Github account
   Go to website http://github.com and create account
3. Docker Jenkins
   ```bash
   docker run -d -p 8181:8080 -p 50000:50000 -v jenkins-data:/var/jenkins_home jenkins/jenkins:lts
   ```
   Access Jenkins from browser http://localhost:8080 then initial the password from:
   ```bash
   /var/jenkins_home/secrets/initialAdminPassword
   ```
   then create user & password for Jenkins 
5. Minikube install for Kubernetes
   ```bash
   # Install minikube
   curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
   sudo install minikube-linux-amd64 /usr/local/bin/minikube
   minikube version
   minikube start --driver=docker   
   ```
7. Kubectl install
   ```bash
   # Install kubectl
   sudo snap install kubectl --classic
   kubectl version --client
   ```
# *Project Flow*
1. Create repo for the project "Jenkins-CICD-K8S-Airflow"
   Login to github account & create new repo for project 
3. Create files for CI/CD Pipeline
   * data for ELT -- sales_record.json
   * Airflow DAG -- sales_elt_dag.py
   * Python test for DAG -- test_dag.py
   * Python need to be install -- requirements.txt
   * Jenkins pipeline -- Jenkinsfile 
5. Create local folder from github repo project
   ```bash
   git clone https://github.com/<user>/Jenkins-CICD-K8S-Airflow.git
   ```
7. Prepare Jenkins for CI/CD Pipeline
   * Login to Jenkins
   * New item -- Pipeline-Name
   * Select item -- Pipeline
   * Configure
     * Description -- Short-description-of-pipeline
     * Triggers -- GitHub hook trigger for GITScm polling
     * Definition -- Pipeline script from SCM
     * SCM -- Git
     * Script path -- Jenkinsfile
    * Apply --> Save 
9. Prepare Github for Jenkins trigger
    * Login to github
    * Setting --> webhook
      * Add webhook
      * Payload URL * -- https://<external_jenkins_url>/github-webhook/
      * Content type * -- Application/json  
11. Minikube for Airflow on Kubernetes
    ```bash
    # Checking active minikube & running
    kubectl get pods -n elt-pipeline
    NAME                                     READY   STATUS    RESTARTS          AGE
    airflow-api-server-5665fc9d59-zt7ls      1/1     Running   53 (24h ago)      14d
    airflow-dag-processor-64445d467f-nrw4t   2/2     Running   30 (24h ago)      14d
    airflow-postgresql-0                     1/1     Running   14 (24h ago)      14d
    airflow-scheduler-748c98ffc6-wt5dw       2/2     Running   39 (24h ago)      14d
    airflow-statsd-75fdf4bc64-lxt8l          1/1     Running   14 (24h ago)      14d
    airflow-triggerer-0                      2/2     Running   30 (24h ago)      14d
    upload-pod                               1/1     Running   149 (8m55s ago)   14d
    ```
