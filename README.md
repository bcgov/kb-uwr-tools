# Kootenay Boundary UWR Tools
[![Lifecycle:Maturing](https://img.shields.io/badge/Lifecycle-Maturing-007EC6)](<Redirect-URL>)


This is a repository of tools for evaluating current conditions of select [GAR UWR orders](https://www.env.gov.bc.ca/wld/frpa/uwr/approved_uwr.html) in the Kootenay-Boundary 
 
---

## UWR  
[4-001](https://www.env.gov.bc.ca/wld/documents/uwr/ord_amend_u-4-001_2020.pdf)   Elk, Mule Deer, White-tailed Deer and Moose  
[4-006](https://www.env.gov.bc.ca/wld/documents/uwr/uwr_u4_006.pdf)   White-tailed Deer, Mule Deer, Moose, Elk, Bighorn Sheep, Mountain Goat  
[4-008](https://www.env.gov.bc.ca/wld/documents/uwr/uwr_u4_008.pdf)	White-tailed Deer, Mule Deer, Moose, Elk, Bighorn Sheep, Mountain Goat  
[8-007](https://www.env.gov.bc.ca/wld/documents/uwr/U-8-007_ord.pdf)   Moose  
[8-008](https://www.env.gov.bc.ca/wld/documents/uwr/U-8-008_ord.pdf)   Mule Deer  
[8-009](https://www.env.gov.bc.ca/wld/documents/uwr/U-8-009_ord.pdf)   Mountain Goat  
[8-010](https://www.env.gov.bc.ca/wld/documents/uwr/U-8-010_ord.pdf)   Sheep  

## Environment Setup  
```
mamba create -n uwrtools -c conda-forge python>=3.11 geospatial jupyterlab jupyter-book
mamba activate uwrtools
mamba install -c esri arcgis>=2.2
```