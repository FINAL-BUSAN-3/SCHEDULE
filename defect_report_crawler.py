from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.alert import Alert
import time
from datetime import datetime
import pandas as pd
# 브라우저 열기
driver = webdriver.Chrome()

# onclick_event 파라미터 값 리스트 (예시로 98426부터 98430까지의 값)

data_list = []

param_values = range(48116, 48121, 1) # 파라미터 값으로 페이지를 찾다보니 자동화에 의문이 듬
if __name__ == "__main__":
    try:
        # 1. 웹 페이지 열기
        driver.get("https://www.car.go.kr/ds/sttemnt/list.do")
        now_time = datetime.now().strftime("%Y-%m-%d")
        # data_list.append("결함발생횟수\t결함발생일\t결함발생시 주행거리(Km)\t결함발생시 속도(Km)\t크롤링시간\t결함신고일\t차명\t차종\t제작(수입)사\t모델연도\t변속기\t엔진배기량(cc)\t사용연료\t결함내용\t파티션_키\n")
        for param_value in param_values:
            try:
                # 2. JavaScript 함수 실행 (2번째 파라미터를 바꿔가며 실행)
                script = f"$main.event.detailView('EP','{param_value}','001');"
                driver.execute_script(script)  # JavaScript 함수 실행
                
                
                try:
                    # alert 대기 시간 설정 (있을 경우에만 처리)
                    time.sleep(2)
                    alert = Alert(driver)
                    alert.accept()  # alert 확인 후 닫기
                    print(f"파라미터 {param_value}에서 발생한 alert을 처리하였습니다.")
                    driver.get("https://www.car.go.kr/ds/sttemnt/list.do")
                    time.sleep(2)   
                except:
                    # alert이 없으면 넘어가기
                    pass
                # 3. 페이지 로딩을 기다리기 (필요시)
                time.sleep(3)  # 페이지 로드 대기
                
                # 4. 현재 페이지 내용 가져오기
                html = driver.page_source
                
                soup = BeautifulSoup(html, 'html.parser')

                # 자동차 결함 정보 크롤링
                defect_count = soup.select_one('table:nth-child(6) > tbody > tr:nth-child(1) > td:nth-child(2)').get_text(separator="\n").strip()
                defect_date = soup.select_one('table:nth-child(6) > tbody > tr:nth-child(1) > td:nth-child(4)').get_text(separator="\n").strip()
                
                defect_distance = soup.select_one('table:nth-child(6) > tbody > tr:nth-child(2) > td:nth-child(2)').get_text(separator="\n").strip()
                defect_distance = "".join(defect_distance.split())
                
                defect_speed = soup.select_one('table:nth-child(6) > tbody > tr:nth-child(2) > td:nth-child(4)').get_text(separator="\n").strip()
                defect_speed = "".join(defect_speed.split())
                
                defect_content = soup.select_one('table:nth-child(6) > tbody > tr:nth-child(3) > td').get_text(separator="\n").strip()
                defect_content = "".join([line.strip() for line in defect_content.split('\n') if line.strip()])

                report_date = soup.select_one("div.info > dl > dd:nth-child(4)").get_text(separator="\n").strip()
                crawl_dt = now_time 
                
                # 자동차 기본 정보 크롤링
                car_name = soup.select_one('table:nth-child(4) > tbody > tr:nth-child(1) > td:nth-child(2)').get_text(separator="\n").strip() 
                car_type = soup.select_one('table:nth-child(4) > tbody > tr:nth-child(1) > td:nth-child(4)').get_text(separator="\n").strip()
                car_manufacturer = soup.select_one('table:nth-child(4) > tbody > tr:nth-child(2) > td:nth-child(2)').get_text(separator="\n").strip()
                car_year = soup.select_one('table:nth-child(4) > tbody > tr:nth-child(2) > td:nth-child(4)').get_text(separator="\n").strip()
                car_transmission = soup.select_one('table:nth-child(4) > tbody > tr:nth-child(3) > td:nth-child(2)').get_text(separator="\n").strip()
                car_engine_capacity = soup.select_one('table:nth-child(4) > tbody > tr:nth-child(4) > td:nth-child(2)').get_text(separator="\n").strip()
                car_fuel = soup.select_one('table:nth-child(4) > tbody > tr:nth-child(4) > td:nth-child(4)').get_text(separator="\n").strip()

                part_dt = crawl_dt[:-3]
               
                data_list.append({
                    'defect_count': defect_count,
                    'defect_date': defect_date,
                    'defect_distance': defect_distance,
                    'defect_speed': defect_speed,
                    'defect_content': defect_content,
                    'report_date': report_date,
                    'crawl_dt': crawl_dt,
                    'car_name': car_name,
                    'car_type': car_type,
                    'car_manufacturer': car_manufacturer,
                    'car_year': car_year,
                    'car_transmission': car_transmission,
                    'car_engine_capacity': car_engine_capacity,
                    'car_fuel': car_fuel,
                    'part_dt': part_dt
                })
                
            


            except Exception as e:
                # 예외가 발생하면 오류 메시지 출력 후 다음 파라미터로 넘어감
                print(f"파라미터 {param_value} 처리 중 오류 발생: {e}")
                continue  # 다음 파라미터로 넘어가기
    finally:
        
        driver.quit()
        df = pd.DataFrame(data_list)
        df.to_csv("car_defect_data.csv", index=False, encoding='utf-8', sep='\x01')
        # 6. 브라우저 닫기