from flask import Flask, render_template, request
import pickle
import numpy as np
from sklearn.preprocessing import StandardScaler
from Logger.log import Logs

log = Logs("test_logs.log")
log.addLog("INFO", "Executing the model !")

model = pickle.load(open('finalized_model.sav', 'rb'))

app = Flask(__name__)

log.addLog("INFO", "Configuring MySQL Database settings !")





@app.route('/')
def man():
    return render_template('home.html')


@app.route('/predict', methods=['POST'])
def home():
    log.addLog("INFO", "Rendering template <home.html> !")
    if request.method =='POST':
        try:
            data1 = request.form['a']
            data2 = request.form['b']
            data3 = request.form['c']
            data4 = request.form['d']
            data5 = request.form['e']
            data6 = request.form['f']
            data7 = request.form['g']
            data8 = request.form['h']
            data9 = request.form['i']
            arr = np.array([[data1, data2, data3, data4, data5, data6, data7,data8,data9]], dtype='float64')
            object = StandardScaler()
            xinput = object.fit_transform(arr)
            pred = model.predict(xinput)
            log.addLog("INFO", "Information collected Successfully !")
            log.addLog("INFO", "final result !")
            return render_template('after.html', data=pred)


        except Exception as e:
            print('The Exception message is: ',e)
            log.addLog("ERROR", "Error occurred while generating model !", e)
            return 'something is wrong'

if __name__ == "__main__":
    app.run(debug=True)


