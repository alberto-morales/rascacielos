#!/bin/bash
date +'%a %b %e %H:%M:%S %Z %Y MAD-BCN' >> /home/oem/bitacora.txt
source /home/oem/anaconda3/etc/profile.d/conda.sh
conda deactivate
conda activate rascacielos
cd /home/oem/workspace/rascacielos/scripts/webscrapper
export PYTHONPATH=/home/oem/workspace/rascacielos:$PYTHONPATH
python3 ../../webscraper/launcher.py -o MAD -d BCN