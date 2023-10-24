from satpy import Scene
from satpy.writers import get_enhanced_image
from satpy.composites import DayNightCompositor
from pyresample.geometry import AreaDefinition
import satpy
import os

class CompositeCreator:
    def __init__(self, nc_directory, timestep):
        self.nc_directory = nc_directory
        nc_file_paths = [self.nc_directory + '/' + f for f in os.listdir(nc_directory) if os.path.isfile(os.path.join(nc_directory, f))]
        self.scn = Scene(filenames=nc_file_paths, reader='abi_l1b')
        self.composite = self.create_composite()
        self.save_to_files(timestep)

    def area_def(self):
        """Define the area for cropping."""
        area_id = 'cal_area'
        description = 'Regional ~1km lat-lon grid'
        proj_dict = {'proj': 'longlat', 'datum': 'WGS84'}
        area_extent = [-140, 20, -115, 45]
        resolution = 0.01
        width = int((area_extent[2] - area_extent[0]) / resolution)
        height = int((area_extent[3] - area_extent[1]) / resolution)

        return AreaDefinition(area_id, description, proj_dict, proj_dict, width, height, area_extent)

    def create_composite(self):
        self.scn.load(['day_microphysics_abi'])
        self.scn.load(["night_microphysics"])
        self.scn = self.scn.resample(self.scn.coarsest_area(), resampler='native')
        cropped_scn = self.scn.resample(self.area_def())
        compositor = DayNightCompositor("day_night_microphysics")
        return compositor([cropped_scn['day_microphysics_abi'], cropped_scn['night_microphysics']])

    def save_to_files(self, timestep):
        year, day, hour = self.nc_directory.split('/')[-3:]
        print(self.nc_directory)
        img = satpy.writers.to_image(self.composite)
        img.save(filename=f'{self.nc_directory}/composite_{year}_{day}_{hour}_{timestep}.png')

