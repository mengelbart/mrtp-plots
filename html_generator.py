from pathlib import Path
import jinja2
import os


def generate_html(input):
    data = []
    dir = Path(input)
    for subdir in dir.iterdir():
        if subdir.is_dir():
            images = sorted(subdir.glob('*.png'))
            data.append({
                'name': subdir.name,
                'plots': [f for f in images],
            })

    # if we got folder of combined plots
    images = sorted(dir.glob("*.png"))
    testtypes = set('_'.join(img.name.split('_')[:-1]) for img in images)

    for testtype in testtypes:
        images_one_tpye = [
            img for img in images if img.name.startswith(testtype)]

        data.append({
            'name': testtype.replace('_', ' ').capitalize(),
            'plots': [f for f in images_one_tpye],
        })

    script_dir = os.path.dirname(os.path.abspath(__file__))
    template_dir = Path(script_dir) / "templates"
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_dir))
    template = env.get_template("index.html")
    rendered_html = template.render(data=data)

    with open('index.html', 'w') as f:
        f.write(rendered_html)
