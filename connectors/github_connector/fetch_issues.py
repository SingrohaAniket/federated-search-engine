import requests

url = "https://api.github.com/repos/microsoft/vscode/issues"

response = requests.get(url)

issues = response.json()

print("Recent GitHub Issues:\n")

for issue in issues[:5]:
    print(issue["title"])