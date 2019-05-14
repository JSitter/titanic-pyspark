FROM jupyter/minimal-notebook
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
CMD ["jupyter-notebook --generate-config"]
CMD ["cat ./jupyter_settings.conf >> ~/.jupyter/jupyter_notebook_config.py"]
CMD ["jupyter-notebook"]