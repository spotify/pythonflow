import pythonflow as pf


with pf.Graph() as graph:
    # Only load the libraries when necessary
    imageio = pf.import_('imageio')
    ndimage = pf.import_('scipy.ndimage')
    np = pf.import_('numpy')

    filename = pf.placeholder('filename')
    image = (imageio.imread(filename).set_name('imread')[..., :3] / 255.0).set_name('image')
    noise_scale = pf.constant(.25, name='noise_scale')
    noise = (1 - np.random.uniform(0, noise_scale, image.shape)).set_name('noise')
    noisy_image = (image * noise).set_name('noisy_image')
    angle = np.random.uniform(-45, 45)
    rotated_image = ndimage.rotate(noisy_image, angle, reshape=False).set_name('rotated_image')
