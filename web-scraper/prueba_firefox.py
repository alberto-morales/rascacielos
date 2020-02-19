from selenium import webdriver
driver = webdriver.Firefox()
driver.get('http://google.com')
print(driver.title)
driver.quit()

'''
apt-get update
apt-get install firefox
pip3 install selenium==3.0.2
wget https://github.com/mozilla/geckodriver/releases/download/v0.14.0/geckodriver-vX.XX.0-linuxXX.tar.gz -O /tmp/geckodriver.tar.gz && tar -C /opt -xzf /tmp/geckodriver.tar.gz && chmod 755 /opt/geckodriver && ln -fs /opt/geckodriver /usr/bin/geckodriver && ln -fs /opt/geckodriver /usr/local/bin/geckodriver
'''