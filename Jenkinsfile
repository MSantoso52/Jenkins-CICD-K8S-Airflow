pipeline {
    agent any
    
    environment {
        // GitHub repo details
        GITHUB_REPO = 'https://github.com/MSantoso52/Jenkins-CICD-K8S-Airflow.git'
        AIRFLOW_NAMESPACE = 'elt-pipeline'
        AIRFLOW_VERSION = '3.0.2'
    }
    
    stages {
        stage('Load - Checkout Code') {
            steps {
                echo '🚀 Loading code from GitHub...'
                git branch: 'main', 
                    url: env.GITHUB_REPO,
                    credentialsId: 'github-credentials'  // Set this up in Jenkins credentials
                
                // Verify files exist
                script {
                    sh 'ls -la'
                    sh 'test -f sales_elt_dag.py || (echo "DAG file missing!" && exit 1)'
                    sh 'test -f test_dag.py || (echo "Test file missing!" && exit 1)'
                    sh 'test -f sales_record.json || (echo "Data file missing!" && exit 1)'
                }
            }
        }
        
        stage('Test - Validate DAG & Data Quality') {
            steps {
                echo '🧪 Running comprehensive DAG tests...'
                
                script {
                    // Install Python dependencies
                    sh '''
                        python3 -m pip install --user --upgrade pip --break-system-packages
                        python3 -m pip uninstall --user numpy pandas --break-system-packages
                        python3 -m pip install --user numpy==1.24.3 --break-system-packages
                        python3 -m pip install --user pytest==7.4.0 apache-airflow==${AIRFLOW_VERSION} pandas==2.0.3 sqlalchemy psycopg2-binary pytest-mock --break-system-packages
                    '''
                    
                    // Run pytest with verbose output and stop on first failure
                    sh '''
                        python3 -m pytest test_dag.py -v --tb=short -x
                    '''
                    
                    // Additional static analysis
                    sh '''
                        echo "Running pylint on DAG..."
                        python3 -m pip install --user pylint --break-system-packages
                        pylint sales_elt_dag.py || true
                        
                        echo "Running flake8 for style checks..."
                        python3 -m pip install --user flake8 --break-system-packages
                        flake8 sales_elt_dag.py || true
                    '''
                    
                    // Test JSON file validity
                    sh '''
                        echo "Validating JSON file..."
                        python3 -c "
import json
with open('sales_record.json', 'r') as f:
    data = json.load(f)
print(f'✓ JSON valid with {len(data)} records')
"
                    '''
                }
            }
            post {
                always {
                    // Archive test results
                    junit '**/pytest/*.xml'
                    publishHTML([
                        allowMissing: false,
                        alwaysLinkToLastBuild: true,
                        keepAll: true,
                        reportDir: 'target/pytest',
                        reportFiles: 'index.html',
                        reportName: 'DAG Test Report'
                    ])
                }
                failure {
                    echo '❌ DAG tests failed! Pipeline stopped.'
                    emailext (
                        subject: "DAG Test FAILED: ${env.JOB_NAME} - ${env.BUILD_NUMBER}",
                        body: "The DAG tests failed. Please check the console output.",
                        to: 'your-email@example.com'  // Configure your email
                    )
                }
            }
        }
        
        stage('Load - Deploy to Airflow on Kubernetes') {
            when {
                expression { currentBuild.result == null || currentBuild.result == 'SUCCESS' }
            }
            steps {
                echo '📤 Deploying DAG to Airflow on Kubernetes...'
                
                script {
                    // Copy DAG to Kubernetes persistent volume or configmap
                    sh '''
                        echo "Copying DAG to Kubernetes cluster..."
                        
                        # Method 1: Copy to persistent volume (if mounted)
                        kubectl cp sales_elt_dag.py ${AIRFLOW_NAMESPACE}/airflow-webserver-xxx:/opt/airflow/dags/ || echo "Webserver copy failed, trying scheduler"
                        kubectl cp sales_elt_dag.py ${AIRFLOW_NAMESPACE}/airflow-scheduler-xxx:/opt/airflow/dags/ || echo "Scheduler copy failed, trying alternative method"
                        
                        # Alternative: Create configmap and restart
                        kubectl create configmap sales-dag-config --from-file=sales_elt_dag.py=sales_elt_dag.py -n ${AIRFLOW_NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -
                        
                        echo "Resyncing Airflow DAGs..."
                        kubectl exec -n ${AIRFLOW_NAMESPACE} deployment/airflow-scheduler -- airflow dags resync
                        kubectl exec -n ${AIRFLOW_NAMESPACE} deployment/airflow-webserver -- airflow dags resync
                        
                        echo "Waiting for DAG to appear..."
                        sleep 10
                        
                        # Verify DAG is loaded
                        echo "Verifying DAG deployment..."
                        kubectl exec -n ${AIRFLOW_NAMESPACE} deployment/airflow-webserver -- airflow dags list | grep sales_elt_dag || echo "DAG may take a moment to appear"
                    '''
                    
                    // Test database connectivity (optional)
                    sh '''
                        echo "Testing PostgreSQL connectivity from Airflow..."
                        kubectl exec -n ${AIRFLOW_NAMESPACE} deployment/airflow-scheduler -- python3 -c "
from sqlalchemy import create_engine
try:
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    with engine.connect() as conn:
        result = conn.execute('SELECT 1')
        print('✓ PostgreSQL connection successful')
except Exception as e:
    print(f'✗ PostgreSQL connection failed: {e}')
"
                    '''
                }
            }
            post {
                success {
                    echo '✅ DAG successfully deployed to Airflow!'
                    // Trigger Airflow DAG test run
                    sh '''
                        echo "Triggering test run of the DAG..."
                        kubectl exec -n ${AIRFLOW_NAMESPACE} deployment/airflow-scheduler -- airflow dags trigger sales_elt_dag --conf '{"dry_run": true}' || echo "Test trigger may take a moment"
                    '''
                    
                    // Update GitHub status
                    updateGitHubStatus status: 'SUCCESS', message: 'DAG deployed successfully to Airflow'
                    
                    emailext (
                        subject: "DAG Deployed SUCCESS: ${env.JOB_NAME} - ${env.BUILD_NUMBER}",
                        body: """
                        The sales_elt_dag has been successfully deployed to Airflow in namespace ${AIRFLOW_NAMESPACE}.
                        
                        View in Airflow: http://your-airflow-url/dags/sales_elt_dag
                        
                        Build: ${env.BUILD_URL}
                        """,
                        to: 'your-email@example.com'
                    )
                }
                failure {
                    echo '❌ DAG deployment failed!'
                    updateGitHubStatus status: 'FAILURE', message: 'DAG deployment to Airflow failed'
                    emailext (
                        subject: "DAG Deploy FAILED: ${env.JOB_NAME} - ${env.BUILD_NUMBER}",
                        body: "The DAG deployment to Airflow failed. Please check the console output.",
                        to: 'your-email@example.com'
                    )
                }
            }
        }
    }
    
    post {
        always {
            echo '🧹 Cleaning up workspace...'
            cleanWs()
        }
        success {
            echo '🎉 Pipeline completed successfully!'
        }
        failure {
            echo '💥 Pipeline failed!'
            // Slack notification (if configured)
            // slackSend(
            //     color: 'danger',
            //     message: "Pipeline ${env.JOB_NAME} #${env.BUILD_NUMBER} failed: ${env.BUILD_URL}"
            // )
        }
    }
}

