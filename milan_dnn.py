import tensorflow as tf

# Model parameters
W = tf.Variable([-2], dtype=tf.float32)
b = tf.Variable([2], dtype=tf.float32)
# Model input and output
x = tf.placeholder(tf.float32)
linear_model = W * x + b
y = tf.placeholder(tf.float32)

# loss
loss = tf.reduce_sum(tf.square(linear_model - y)) # sum of the squares
# optimizer
optimizer = tf.train.AdamOptimizer()
train = optimizer.minimize(loss)

# training data
x_train = [1, 2, 3, 4,5,6,7,8,9,10]
y_train = [0, -1, -2, -3,-4,-5,-6,-7,-8,-9]
# training loop
init = tf.global_variables_initializer()

sess = tf.Session()
sess.run(init) # reset values to wrong
print(sess.run(W))
print(sess.run(b))

for i in range(10):
  curr_W = sess.run(W)
  curr_b = sess.run(b)
  curr_loss = sess.run(loss, {x: [i + 1], y: [-i]})
  print("Pre W: %s b: %s loss: %s" % (curr_W, curr_b, curr_loss))

  sess.run(train, {x: [i+1], y: [-i]})
  curr_W = sess.run(W)
  curr_b = sess.run(b)
  curr_loss = sess.run(loss, {x: [i+1], y: [-i]})
  print("Posle W: %s b: %s loss: %s" % (curr_W, curr_b, curr_loss))


sess.run(init) # reset values to wrong
print(sess.run(W))
print(sess.run(b))

for i in range(20000):
  #for i in range(10):
  #  curr_loss = sess.run(loss, {x: [i + 1], y: [-i]})
  #  print("loss: %s" % (curr_loss))
  #curr_loss = sess.run(loss, {x: x_train, y: y_train})
  #curr_W = sess.run(W)
  #curr_b = sess.run(b)
  #print("Pre Ukupno W: %s b: %s loss: %s" % (curr_W, curr_b, curr_loss))

  sess.run(train, {x: x_train, y: y_train})
  # evaluate training accuracy
curr_loss = sess.run(loss, {x: x_train, y: y_train})
curr_W = sess.run(W)
curr_b = sess.run(b)
print("Posle Ukupno W: %s b: %s loss: %s"%(curr_W, curr_b, curr_loss))

