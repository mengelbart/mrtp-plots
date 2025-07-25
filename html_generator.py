import jinja2
import glob
import os


def generate_html(input):
    image_paths = glob.glob(f'{input}/*/rtp_rate.png', recursive=True)
    images = []
    for path in image_paths:
        html_path = path.replace(os.sep, '/')
        dir_name = os.path.basename(os.path.dirname(path))
        images.append({'path': html_path, 'caption': dir_name})

    env = jinja2.Environment(loader=jinja2.FileSystemLoader("templates"))
    template = env.get_template("index.html")

    rendered_html = template.render(images=images)

    with open('index.html', 'w') as f:
        f.write(rendered_html)
