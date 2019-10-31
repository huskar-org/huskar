pipeline {
  agent {
    dockerfile {
      filename 'jenkins.dockerfile'
      args '-v /data/jenkins-volumes/pip:/home/jenkins/.cache/pip'
    }
  }
  environment {
    PYTHONUNBUFFERED = '1'
    GEVENT_RESOLVER = 'block'
    PIP_INDEX_URL = 'https://pypi.doubanio.com/simple/'
    DATABASE_DEFAULT_URL = "mysql+pymysql://root@127.0.0.1:3306/huskar_api?charset=utf8mb4"
    HUSKAR_API_DB_URL  = "mysql+pymysql://root@127.0.0.1:3306/huskar_api?charset=utf8mb4"
    REDIS_DEFAULT_URL = "redis://127.0.0.1:6379"
    HUSKAR_API_REDIS_URL = "redis://127.0.0.1:6379"

    HUSKAR_API_DEBUG = 'true'
    HUSKAR_API_TESTING = 'true'
    HUSKAR_API_SECRET_KEY = 'test-secret-key'
    HUSKAR_API_ZK_SERVERS = "127.0.0.1:2181"
  }
  stages {
    stage('Install') {
      steps {
        sh 'mysqld_safe --user=jenkins --skip-grant-tables &'
        sh 'redis-server &'
        sh 'zookeeper-server start-foreground &'

        sh 'make install-deps'
        sh './manage.sh initdb'
      }
    }
    stage('Test') {
      steps {
        sh './manage.sh lint'
        sh './manage.sh testonly tests -xv --junitxml=junit.xml --cov=huskar_api --cov-report term-missing'
        sh 'coverage xml'
        sh 'coverage html'
        sh 'coverage report --show-missing'
        sh 'test "$(coverage report | tail -n1 | awk \'{print $6}\')" = "100%"'
      }
    }
    stage('Build Doc') {
      steps {
        sh 'make -C docs html'
        archiveArtifacts artifacts: 'docs/_build/html/**', fingerprint: true
      }
      when {
        anyOf {
          branch 'master'
          changeset '**/*.rst'
          changelog 'Docs:.+'
          changeRequest title: 'Docs:.+', comparator: 'REGEXP'
          changeRequest branch: 'docs/*', comparator: 'GLOB'
        }
      }
    }
  }
  post {
    success {
      junit 'junit.xml'
      cobertura coberturaReportFile: 'coverage.xml'
      publishHTML(target: [
        allowMissing: false,
        alwaysLinkToLastBuild: false,
        keepAll: true,
        reportDir: 'htmlcov',
        reportFiles: 'index.html',
        reportName: 'Coverage Report'
      ])
    }
  }
}
