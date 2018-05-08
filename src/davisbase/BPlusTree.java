/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package davisbase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author konchady
 */
class BPlusTree {
    
    static Long cptr = null;
    static String tableName = "";
    protected final long PAGE_SIZE = 512L;
    
    public boolean writeLeafHeader(long pageStart, long pageEnd, int recordSize, int rightPointer) {
        
        try {
            RandomAccessFile tableraf = new RandomAccessFile(tableName, "rw");
            tableraf.seek(pageStart);
            tableraf.writeByte(1);
            tableraf.writeByte(1);
            tableraf.writeShort((int) (pageEnd - recordSize));
            tableraf.writeInt(rightPointer);
            tableraf.writeShort((int) (pageEnd - recordSize));
            cptr = Long.valueOf(pageEnd - recordSize);
            tableraf.close();
            return true;            
            
            
        } catch (FileNotFoundException ex) {
            Logger.getLogger(BPlusTree.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(BPlusTree.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }
    
	public boolean updateLeafHeader(long pageStart, long pageEnd, int recordSize, int rightPointer) {
		try {
			RandomAccessFile tableraf = new RandomAccessFile(tableName, "rw");
			tableraf.seek(pageStart);
			tableraf.readByte();

			int cells = tableraf.readByte() + 1;
			tableraf.seek(pageStart + 1L);
			tableraf.writeByte(cells);

			int oldCellAddress = tableraf.readShort();
			int newCellAddress = oldCellAddress - recordSize;
			tableraf.seek(pageStart + 2L);
			tableraf.writeShort(newCellAddress);
			tableraf.writeInt(rightPointer);

			short[] cellAddress = new short[cells];
			for (int i = 0; i < cells - 1; i++) {
				cellAddress[i] = tableraf.readShort();
			}
			tableraf.writeShort(newCellAddress);
			cptr = Long.valueOf(newCellAddress);
			tableraf.close();
			return true;
		} catch (Exception e) {
		}
		return false;
	}

	public boolean updateRightPointerOfLeafHeader(long pageStart, long pageEnd, int recordSize, int rightPointer) {
		try {
			RandomAccessFile tableraf = new RandomAccessFile(tableName, "rw");
			tableraf.seek(pageStart);
			tableraf.readByte();
			tableraf.readByte();
			tableraf.readShort();
			tableraf.writeInt(rightPointer);
			tableraf.close();
			return true;
		} catch (Exception e) {
		}
		return false;
	}

	public int getLastId(long pageStart) {
		try {
			int lastId = 0;
			RandomAccessFile tableraf = new RandomAccessFile(tableName, "rw");
			tableraf.seek(pageStart + PAGE_SIZE);
			tableraf.readByte();
			tableraf.readByte();
			int lastAddress = tableraf.readShort();
			tableraf.writeInt(0);
			tableraf.seek(lastAddress);
			lastId = tableraf.readInt();
			tableraf.close();
			return lastId;
		} catch (Exception e) {
		}
		return 0;
	}

	public void checkInteriorPageOverflow(long pagePointer) {
		if (pagePointer != 0L) {
			try {
				RandomAccessFile tableraf = new RandomAccessFile(tableName, "rw");
				int noOfPages = 0;
				tableraf.readByte();
				tableraf.readInt();
				tableraf.readInt();
				tableraf.readInt();
				long rightMostPointer = tableraf.readInt();
				long parentPointer = tableraf.readInt();
				long OverflowBucket = 0L;
				int id = 0;
				while (rightMostPointer != 0L) {
					noOfPages++;
					if (3 == noOfPages) {
						OverflowBucket = rightMostPointer;
						tableraf.readByte();
						tableraf.readInt();
						id = tableraf.readInt();
						break;
					}
					tableraf.seek(rightMostPointer);
					tableraf.readByte();
					tableraf.readInt();
					tableraf.readInt();
					tableraf.readInt();
					rightMostPointer = tableraf.readInt();
				}
				if (OverflowBucket != 0L) {
					long newPageStart = PAGE_SIZE * (tableraf.length() / PAGE_SIZE + 2L);
					long topParentPointer = 0L;
					if (parentPointer == 0L) {
						parentPointer = newPageStart;
					} else {
						tableraf.seek(parentPointer);
						tableraf.readByte();
						tableraf.readInt();
						tableraf.readInt();
						tableraf.readInt();
						tableraf.writeInt((int) newPageStart);

						topParentPointer = tableraf.readInt();
					}

					tableraf.seek(newPageStart);
					tableraf.readByte();
					tableraf.writeInt((int) pagePointer);
					tableraf.writeInt(id);
					tableraf.writeInt((int) OverflowBucket);
					tableraf.writeInt((int) topParentPointer);

					tableraf.seek(pagePointer);
					tableraf.readByte();
					tableraf.readInt();
					tableraf.readInt();
					tableraf.readInt();
					rightMostPointer = tableraf.readInt();
					tableraf.writeInt((int) parentPointer);

					tableraf.seek(pagePointer + PAGE_SIZE);
					tableraf.readByte();
					tableraf.readInt();
					tableraf.readInt();
					tableraf.writeInt(0);

					while (rightMostPointer != 0L) {
						tableraf.seek(rightMostPointer);
						tableraf.readByte();
						tableraf.readInt();
						tableraf.readInt();
						tableraf.readInt();
						rightMostPointer = tableraf.readInt();
						tableraf.writeInt((int) parentPointer);
					}
					checkInteriorPageOverflow(parentPointer);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void checkOverflow() {
		try {
			RandomAccessFile tableraf = new RandomAccessFile(tableName, "rw");
			long newPageStart = PAGE_SIZE * (tableraf.length() / PAGE_SIZE + 2L);
			if (tableraf.length() > PAGE_SIZE * 3L) {
				long pagePointer = -1L;
				while (tableraf.getFilePointer() < tableraf.length()) {
					int pageType = tableraf.readByte();
					if (pageType == 0) {
						pagePointer = tableraf.getFilePointer() - 1L;
						break;
					}
					tableraf.seek(tableraf.getFilePointer() + PAGE_SIZE - 1L);
				}

				if (pagePointer == -1L) {
					long leftPointer = 0L;
					int lastId = getLastId(0L);
					tableraf.seek(newPageStart);
					tableraf.writeByte(0);
					tableraf.writeInt(0);
					tableraf.writeInt(lastId + 1);
					tableraf.writeInt((int) (leftPointer + PAGE_SIZE * 3L));
					tableraf.writeInt(0);
					tableraf.writeInt(0);
					tableraf.close();
				} else {
					tableraf.seek(pagePointer);
					int pageType = tableraf.readByte();
					tableraf.readInt();
					tableraf.readInt();
					long rightPointer = tableraf.readInt();
					long rightMostPointer = tableraf.readInt();

					tableraf.seek(rightPointer);
					pageType = tableraf.readByte();
					while (pageType != 1) {
						tableraf.readInt();
						tableraf.readInt();
						rightPointer = tableraf.readInt();
						rightMostPointer = tableraf.readInt();
						tableraf.seek(rightPointer);
						pageType = tableraf.readByte();
					}

					while (rightMostPointer != 0L) {
						pagePointer = rightMostPointer;
						tableraf.seek(rightMostPointer);
						pageType = tableraf.readByte();
						tableraf.readInt();
						tableraf.readInt();
						rightPointer = tableraf.readInt();
						rightMostPointer = tableraf.readInt();
					}

					tableraf.seek(rightPointer);
					int noOfPages = 1;
					long overflowBucket = 0L;
					long lastLeafPagePointer = rightPointer;
					tableraf.readByte();
					tableraf.readByte();
					tableraf.readShort();
					rightMostPointer = tableraf.readInt();
					while (rightMostPointer != 0L) {
						tableraf.seek(rightMostPointer);
						noOfPages++;
						if (noOfPages == 4) {
							overflowBucket = rightMostPointer;
							break;
						}
						tableraf.readByte();
						tableraf.readByte();
						tableraf.readShort();
						rightMostPointer = tableraf.readInt();
					}

					if (overflowBucket != -1L) {
						tableraf.seek(lastLeafPagePointer);
						tableraf.readByte();
						tableraf.readInt();
						tableraf.readInt();
						rightPointer = tableraf.readInt();
						tableraf.writeInt((int) newPageStart);
						long parentPointer = tableraf.readInt();

						tableraf.seek(lastLeafPagePointer + PAGE_SIZE);
						tableraf.readByte();
						tableraf.readInt();
						tableraf.readInt();
						tableraf.writeInt(0);

						int lastId = getLastId(lastLeafPagePointer);
						tableraf.seek(newPageStart);
						tableraf.writeByte(0);
						tableraf.writeInt((int) rightPointer);
						tableraf.writeInt(lastId + 1);
						tableraf.writeInt((int) overflowBucket);
						tableraf.writeInt(0);
						tableraf.writeInt((int) parentPointer);

						checkInteriorPageOverflow(rightPointer);
					}
				}
				tableraf.close();
			}
			tableraf.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public long insert(int recordSize) {
		try {
			RandomAccessFile tableraf = new RandomAccessFile(tableName, "rw");
			long filelen = tableraf.length();
			long fileIndex = tableraf.getFilePointer();
			long pageStart = 0L;
			long pageEnd = pageStart + PAGE_SIZE - 1L;
			if (filelen == 0L) {
				tableraf.close();
				writeLeafHeader(pageStart, pageEnd, recordSize, 0);
				return cptr.longValue();
			}

			int pageType = tableraf.readByte();
			int cells = tableraf.readByte();
			int startPointer = tableraf.readShort();
			int rightPointer = tableraf.readInt();
			while (rightPointer != 0) {
				tableraf.seek(rightPointer);
				pageType = tableraf.readByte();
				cells = tableraf.readByte();
				startPointer = tableraf.readShort();
				rightPointer = tableraf.readInt();
			}

			if ((pageType == 1) && (rightPointer == 0)) {
				fileIndex = tableraf.getFilePointer();
				tableraf.close();
				pageStart = fileIndex - 8L;
				pageEnd = pageStart + PAGE_SIZE;
				cells++;
				if (startPointer - recordSize > pageStart + 8L + 2 * cells) {
					updateLeafHeader(pageStart, pageEnd, recordSize, rightPointer);
				} else {
					rightPointer = (int) ((filelen + 1L) / PAGE_SIZE * PAGE_SIZE);
					updateRightPointerOfLeafHeader(pageStart, pageEnd, recordSize, rightPointer);
					writeLeafHeader(rightPointer, rightPointer + PAGE_SIZE - 1L, recordSize, 0);
					checkOverflow();
				}
			}
			tableraf.close();
			return cptr.longValue();
		} catch (Exception e) {
		}
		return 0L;
	}    
    
    
}
