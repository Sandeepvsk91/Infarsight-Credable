from flask import Flask, request, send_from_directory, redirect, url_for, render_template_string

app = Flask(__name__)

# Set your login credentials
USERNAME = "admin"
PASSWORD = "1234"

# Local directory with PDFs
PDF_FOLDER = "pdfs"

# In-memory session (very basic)
sessions = set()

# Simple HTML templates
login_template = '''
<form method="post">
  <h2>Login</h2>
  <input name="username" placeholder="Username"><br>
  <input name="password" type="password" placeholder="Password"><br>
  <button type="submit">Login</button>
</form>
'''

file_list_template = '''
<h2>Available PDF Files</h2>
<ul>
  {% for file in files %}
    <li><a href="/pdf/{{ file }}">{{ file }}</a></li>
  {% endfor %}
</ul>
'''

@app.route('/', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        if request.form['username'] == USERNAME and request.form['password'] == PASSWORD:
            sessions.add(request.remote_addr)
            return redirect(url_for('list_pdfs'))
        return "Invalid credentials", 401
    return render_template_string(login_template)

@app.route('/files')
def list_pdfs():
    if request.remote_addr not in sessions:
        return redirect('/')
    import os
    files = [f for f in os.listdir(PDF_FOLDER) if f.endswith('.pdf')]
    return render_template_string(file_list_template, files=files)

@app.route('/pdf/<filename>')
def serve_pdf(filename):
    if request.remote_addr not in sessions:
        return redirect('/')
    return send_from_directory(PDF_FOLDER, filename)

if __name__ == '__main__':
    app.run(debug=True)
