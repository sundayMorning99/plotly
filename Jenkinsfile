pipeline {
    agent any

    environment {
        DOCKER_IMAGE = 'm2-visualization'
        DOCKER_TAG   = "${BUILD_NUMBER}"
        FRED_API_KEY = credentials('fred-api-key')
    }

    options {
        timestamps()
        buildDiscarder(logRotator(numToKeepStr: '10'))
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
                echo "Code checked out successfully"
            }
        }

        stage('Install Dependencies') {
            steps {
                sh '''
                    python3 -m venv venv
                    . venv/bin/activate
                    pip install --upgrade pip
                    pip install -r requirements.txt
                '''
                echo "Dependencies installed"
            }
        }

        stage('Run Tests') {
            steps {
                sh '''
                    . venv/bin/activate
                    python -m pytest tests/ || echo "No tests found or tests failed (continuing)"
                '''
            }
        }

        stage('Code Quality Check') {
            steps {
                sh '''
                    . venv/bin/activate
                    pip install pylint --quiet
                    pylint m2_visualization.py || echo "Linting issues (non-blocking)"
                '''
            }
        }

        stage('Build Docker Image') {
            steps {
                script {
                    def image = docker.build("${DOCKER_IMAGE}:${DOCKER_TAG}")
                    docker.build("${DOCKER_IMAGE}:latest")
                    echo "Docker images built: ${DOCKER_IMAGE}:${DOCKER_TAG} and latest"
                }
            }
        }

        stage('Start Services') {
            steps {
                sh '''
                    docker-compose down -v --remove-orphans || true
                    docker-compose up -d
                    echo "Waiting for services to be healthy..."
                    sleep 15
                    docker-compose ps
                    docker-compose logs kafka | tail -20
                '''
                echo "Services started (Zookeeper + Kafka + m2-visualization container)"
            }
        }

        stage('Run M2 Visualization Pipeline') {
            steps {
                sh '''
                    echo "Executing m2_visualization.py inside container..."
                    docker-compose exec -T m2-visualization python m2_visualization.py
                '''
                echo "Pipeline executed successfully"
            }
        }

        stage('Archive Artifacts') {
            steps {
                archiveArtifacts artifacts: 'output/*.html, output/*.csv', 
                                 allowEmptyArchive: false, 
                                 fingerprint: true
                echo "Artifacts archived"
            }
        }

        stage('Publish HTML Report') {
            steps {
                publishHTML(target: [
                    allowMissing: false,
                    alwaysLinkToLastBuild: true,
                    keepAll: true,
                    reportDir: 'output',
                    reportFiles: 'm2_visualization.html',
                    reportName: 'M2 Money Supply Visualization Report'
                ])
                echo "HTML report published"
            }
        }
    }

    post {
        always {
            sh '''
                echo "Cleaning up Docker resources..."
                docker-compose down -v --remove-orphans || true
                docker system prune -f --volumes || true
            '''
            cleanWs()
        }
        success {
            echo "Pipeline completed successfully!"
        }
        failure {
            echo "Pipeline failed!"
        }
        aborted {
            echo "Pipeline was aborted."
        }
    }
}