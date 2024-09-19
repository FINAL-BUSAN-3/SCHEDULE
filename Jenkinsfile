pipeline {

	agent any
	stages {
		stage('[Schedule Sync] Start') {
			steps {
				 sh '[Schedule Sync] Start'
				// slackSend(channel: '#deployment-alert', color: '#00FF7F' , message: "[Schedule Sync] Start : Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
			}
		}
		stage('[Schedule Sync] Git clone') {
                        steps {
				sh '[Schedule Sync] Git clone'
				sh 'ssh -i /opt/och_aws.pem ubuntu@ec2-3-105-131-178.ap-southeast-2.compute.amazonaws.com `cd /home/ubuntu/airflow-compose/dags && git clone https://github.com/FINAL-BUSAN-3/SCHEDULE.git`'
                                // slackSend(channel: '#deployment-alert', color: '#00FF7F' , message: "[Schedule Sync] Git clone : Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
                        }
                }
		stage('[Schedule Sync] Done') {
                        steps {
				 sh 'echo "Hello"'
                                // slackSend(channel: '#deployment-alert', color: '#00FF7F' , message: "[Schedule Sync] Done : Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
                        }
                }
		
	}
}
