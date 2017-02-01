import theano
from keras.models import Sequential
from keras.layers import Dense, Activation

x = theano.tensor.dscalar()
f = theano.function([x], 2*x)
f(4)

model = Sequential([
    Dense(32, input_dim=784),
    Activation('relu'),
    Dense(10),
    Activation('softmax'),
])
model = Sequential()
model
