from tqdm import tqdm

# A function to indicate process
class Progress:
  def __init__(self, total):
    self.progress = 0
    self.total = total
    print(total)
    self.pbar = tqdm(total=total)

  def count(self):
    self.progress += 1
    if self.finished():
      self.pbar.close()
    else:
      self.pbar.update(1)

  def finished(self):
    return self.progress >= self.total