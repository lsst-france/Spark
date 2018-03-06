
import matplotlib.pyplot as plt

image = plt.imread('test.png')

fig, ax = plt.subplots()
im = ax.imshow(image)
plt.show()

