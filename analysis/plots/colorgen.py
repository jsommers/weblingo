"""
grayscale color generator
"""

def colorgen(numcolors=6):
    maxx = 160
    step = maxx/numcolors
    d = numcolors+2
    evens = [(i*step)/maxx for i in range(numcolors) if i % 2 == 0]
    odds = [(i*step)/maxx for i in range(numcolors) if i % 2 != 0]
    colors = evens + odds

    i = 0
    while True:
        yield f"{colors[i]}"
        i += 1
        if i == len(colors):
            i = 0


if __name__ == '__main__':
    cg = colorgen()
    for i in range(10):
        print(next(cg))

