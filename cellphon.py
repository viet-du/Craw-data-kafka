import os
import json,cv2,requests
import requests
import random
import time
import numpy as np
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urljoin
import time, random
from selenium import webdriver
from selenium.webdriver.common.by import By
#thêm kafka
from kafka_client import get_producer
import cv2, requests, numpy as np

producer = get_producer()

# ví dụ sau khi crawl được ảnh
filename = "images-cellphone/product_1.jpg"
message = {
    "id": 1,
    "file_path": filename,
    "url": "https://example.com/image1.jpg",
    "width": 640,
    "height": 480
}

producer.send("cellphone-topic", value=message)
print(f"Đã gửi vào Kafka: {message}")


#=========Đường dẫn trang wed==========
driverpath = r"D:\chromedriver-win64\chromedriver.exe"
service = Service(driverpath)
driver = webdriver.Chrome(service=service)
#==========links========================
driver.get("https://cellphones.com.vn/mobile/dien-thoai-pin-trau.html")
time.sleep(random.randint(5, 10))
#Craw thông tin
# tạo thư mục lưu ảnh
os.makedirs("images-cellphone", exist_ok=True)
#tên sp
elems = WebDriverWait(driver, 10).until(
    EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".product__name"))
)
elems=driver.find_elements(By.CSS_SELECTOR,".product__name")
title_all = [elem.text for elem in elems]
elems=driver.find_elements(By.CSS_SELECTOR,"[href]")
links_all = [elem.get_attribute("href") for elem in elems]
#giá
#giá
try:
    # Lấy giá đã giảm
    try:
        prices_show = driver.find_elements(By.CSS_SELECTOR, ".product__price--show")
        prices_through = driver.find_elements(By.CSS_SELECTOR, ".product__price--through")

        all_price1 = [elem.text for elem in prices_show]
        all_price2 = [elem.text for elem in prices_through]

        for show, through in zip(all_price1, all_price2):
            print(f"Giá đã giảm: {show} | Giá gốc: {through}")
    except Exception as e:
        print(" Không thể lấy giá:", e)

    # Lấy badge (đặc điểm)
    try:
        elems = driver.find_elements(By.CSS_SELECTOR, ".product__badge")
        product = [elem.text for elem in elems]
        print("Badge:", product)
    except Exception as e:
        print(" Không thể lấy badge:", e)

    # Lấy phần trăm giảm giá
    try:
        elems = driver.find_elements(By.CSS_SELECTOR, ".product__price--percent-detail")
        discount_all = [elem.text for elem in elems]
        print("Giảm giá:", discount_all)
    except Exception as e:
        print(" Không tìm thấy phần trăm giảm giá:", e)

except Exception as e:
    print(" Lỗi tổng thể:", e)

# lấy danh sách thẻ img
# tạo thư mục lưu ảnh
os.makedirs("images-cellphone", exist_ok=True)
images = driver.find_elements(By.CSS_SELECTOR, ".product__img")
img_links = [img.get_attribute("src") for img in images]

for idx, link in enumerate(img_links, start=1):
    try:
        response = requests.get(link, timeout=10)
        if response.status_code == 200:
            img_array = np.asarray(bytearray(response.content), dtype=np.uint8)
            img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)

            if img is not None:
                filename = f"images-cellphone/product_{idx}.jpg"  # 👈 sửa chỗ này
                cv2.imwrite(filename, img)
                print(f" Đã lưu: {filename} ({img.shape[1]}x{img.shape[0]})")
            else:
                print(f" Không đọc được ảnh {idx} từ link: {link}")
        else:
            print(f" Lỗi HTTP {response.status_code} khi tải ảnh {idx}")
    except Exception as e:
        print(f"Lỗi khi tải ảnh {idx}: {e}")
# ================= Lưu JSON =================
data = []
for i in range(len(title_all)):
    item = {
        "title": title_all[i] if i < len(title_all) else "",
        "link": links_all[i] if i < len(links_all) else "",
        "price_after": all_price1[i] if i < len(all_price1) else "",
        "price_before": all_price2[i] if i < len(all_price2) else "",
        "badge": product[i] if i < len(product) else "",
        "discount": discount_all[i] if i < len(discount_all) else "",
        "image_url": img_links[i] if i < len(img_links) else "",
        "image_file": f"images-cellphone/product_{i+1}.jpg" if i < len(img_links) else ""
    }
    data.append(item)

with open("cellphone_products.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=4)

print(f"\n Đã lưu {len(data)} sản phẩm vào cellphone_products.json")
# ================= Gửi vào Kafka =================
for idx, item in enumerate(data, start=1):
    producer.send("cellphone-topic", value=item)
    print(f"Đã gửi vào Kafka: {item}")


producer.flush()

