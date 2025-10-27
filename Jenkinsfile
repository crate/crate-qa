// https://www.jenkins.io/doc/pipeline/tour/agents/
// https://www.jenkins.io/doc/book/pipeline/docker/
// https://www.jenkins.io/doc/book/pipeline/syntax/
pipeline {
  agent any
  options {
    timeout(time: 4, unit: 'HOURS')
  }
  stages {
    stage('Parallel') {
      parallel {
        stage('Lint python code') {
          agent { label 'medium && x64' }
          steps {
            checkout scm
            sh 'rm -rf env'
            sh '/usr/bin/python3.11 -m venv env'
            sh 'env/bin/python -m pip install -U mypy flake8'
            sh 'find tests -name "*.py" | xargs env/bin/mypy --ignore-missing-imports'
            sh 'find src -name "*.py" | xargs env/bin/mypy --ignore-missing-imports'
            sh 'env/bin/flake8 src/ tests/'
          }
        }
        stage('Python bwc-rolling-upgrade tests 5-6') {
          agent { label 'medium && x64' }
          tools { jdk 'jdk11' }
          steps {
            checkout scm
            sh '''
              rm -rf env
              uv venv env
              source env/bin/activate
              uv pip install -U -e .

              (cd tests/bwc && python -m unittest -vvv test_rolling_upgrade.RollingUpgradeTest.test_rolling_upgrade_5_to_6)
            '''
          }
        }
      }
    }
  }
  post {
    cleanup {
      deleteDir()  /* clean up our workspace */
    }
  }
}
