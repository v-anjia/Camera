'''
capture image from usb device camera
author :antony weijiang
date:2019/9/11
'''
import cv2
import os
import time

if __name__ == "__main__":
    try:
        cap = cv2.VideoCapture(cv2.CAP_DSHOW)

        print("摄像头是否已经打开 ？ {}".format(cap.isOpened()))
        #set picture size
        cap.set(cv2.CAP_PROP_FRAME_WIDTH, 1920)
        cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 1080)
        cv2.namedWindow('Camera', flags=cv2.WINDOW_NORMAL | cv2.WINDOW_KEEPRATIO | cv2.WINDOW_GUI_EXPANDED)
        img_count = 1
        while cap.isOpened():
            ret, frame = cap.read()

            if not ret:
                print("get image fail ,maybe can not camera devices")
                break
            cv2.imshow('Camera', frame)
    
            # 等待按键事件发生 等待1ms
            key = cv2.waitKey(1)
            if key == 27:
                #按esc键退出
                print("exit progress,stop capture message")
                break
            elif key == 32:
                #按空格键截图
                date_time = time.strftime('%Y-%m-%d__%H-%M-%S',time.localtime(time.time()))
                cv2.imwrite("caputure_{0}_{1}.png".format(date_time,img_count), frame)
                print("save picture name:caputure_{0}_{1}.png".format(date_time,img_count))
                img_count += 1
        cap.release()
        cv2.destroyAllWindows()
    except Exception as e:
        print("%s" %(e))
