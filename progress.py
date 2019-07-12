from tqdm import tqdm
import math

# A function to indicate process
class Progress:
  def __init__(self, total, batches=1):
    self.progress = 0
    self.current_batch = 1
    self.total = total
    self.batches = batches # Process data in batches to avoid crashings
    self.items_per_batch = math.ceil(float(total) / float(batches))
    print('Total Items:', self.total)
    print('Items Per Batch:', self.items_per_batch)
    self.pbar = tqdm(total=total, desc='Total Items')
    self.bbar = tqdm(total=batches, initial=1, desc='Current Batches')

  def count(self):
    self.progress += 1
    self.pbar.update(1)
    if self.progress % self.items_per_batch == 0:
      self.current_batch += 1
      self.bbar.update(1)
    if self.finished():
      self.pbar.close()
      self.bbar.close()

  def finished(self):
    return self.progress >= self.total

  def is_batch_complete(self):
    return (self.current_batch == self.batches and self.finished() or
      self.progress % self.items_per_batch == 0)