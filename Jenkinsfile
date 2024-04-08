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
        stage('Rust client tests') {
          agent {
            dockerfile {
              label 'docker && x64'
            }
          }
          steps {
            sh '''
              export HOME=$(pwd)
              export LANG=en_US.UTF-8
              test -d env && rm -rf env
              python3 -m venv env
              . env/bin/activate
              python -m pip install -U cr8
              cd graaldemo
              ./mvnw -T1C clean package
              java -jar target/demo-1-jar-with-dependencies.jar
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
