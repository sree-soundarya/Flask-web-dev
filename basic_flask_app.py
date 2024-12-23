from flask import Flask,redirect,url_for

app = Flask(__name__)

@app.route('/')
def welcome():
    return "Welcome to my web app. This is my first web-app"

@app.route('/pass_marks/<int:marks>') #building a dynamic url
def pass_marks(marks):
    return f"student passed with {str(marks)} in maths"

@app.route('/fail/<int:marks>') #building a dynamic url
def fail(marks):
    return f"student failed with {str(marks)} in maths"

@app.route('/results/<int:score>')
def results(score):
    assert score>0
    if score >40:
        result= 'pass_marks'
    else:
        result =  'fail'\
    return redirect(url_for(result,marks=score))

if __name__ == "__main__":
    app.run(debug=True)