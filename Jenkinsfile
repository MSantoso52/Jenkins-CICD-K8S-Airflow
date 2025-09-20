pipeline {
    agent any  // Or use 'agent { kubernetes { ... } }' if on K8s
    stages {
        stage('Load (Checkout)') {
            steps {
                checkout scm
                echo 'Loaded code from GitHub'
            }
        }
        stage('Test') {
            steps {
                sh 'pip install -r requirements.txt'  // Install deps
                sh 'pytest test_sales_elt_dag.py'    // Run tests
                echo 'DAG tests passed'
            }
        }
        stage('Load (Deploy to Airflow)') {
            when {
                expression { currentBuild.currentResult == 'SUCCESS' }
            }
            steps {
                // Assume Kubernetes cluster with Airflow namespace 'airflow'
                // and a pod 'airflow-webserver-xxx' with DAGs at /opt/airflow/dags
                // Adjust pod name via 'kubectl get pods -n airflow'
                sh '''
                kubectl cp sales_elt_dag.py airflow/airflow-webserver-pod:/opt/airflow/dags/sales_elt_dag.py
                echo 'DAG deployed to Airflow'
                '''
            }
        }
    }
    post {
        always {
            echo 'Pipeline completed'
        }
    }
}
