pipeline {

	agent any
	stages {
		stage('[Schedule Sync] Start') {
			steps {
				 sh 'echo "[Schedule Sync] Start"'
				// slackSend(channel: '#deployment-alert', color: '#00FF7F' , message: "[Schedule Sync] Start : Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
			}
		}
		stage('[Schedule Sync] Git clone') {
                        steps {
				//def localUser = 'ubuntu'
				//def localHost = 'ec2-3-105-131-178.ap-southeast-2.compute.amazonaws.com'
				//def pemPath = '/opt/och_aws.pem'				

				sh 'echo "[Schedule Sync] Git clone"'
				//sh """
				//ssh -i ${pemPath} ${localUser}@${localHost} "ls"
				//"""
				//"cd /home/ubuntu/airflow-compose/dags/SCHEDULE && git pull"
				//"""
                                // slackSend(channel: '#deployment-alert', color: '#00FF7F' , message: "[Schedule Sync] Git clone : Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
			}
                }
		stage('[Schedule Sync] Done') {
                        steps {
				sh 'echo "[Schedule Sync] Done"'
                                // slackSend(channel: '#deployment-alert', color: '#00FF7F' , message: "[Schedule Sync] Done : Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
			}
                }
		
	}
}
