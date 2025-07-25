from pathlib import Path
import jinja2


def generate_html(input):
    data = []
    for subdir in Path(input).iterdir():
        if subdir.is_dir():
            images = sorted(subdir.glob('*.png'))
            data.append({
                'name': subdir.name,
                'plots': [f for f in images],
            })

    env = jinja2.Environment(loader=jinja2.FileSystemLoader("templates"))
    template = env.get_template("index.html")
    rendered_html = template.render(data=data)

    with open('index.html', 'w') as f:
        f.write(rendered_html)
