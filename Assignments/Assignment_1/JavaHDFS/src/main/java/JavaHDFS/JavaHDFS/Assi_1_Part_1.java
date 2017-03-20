package JavaHDFS.JavaHDFS;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;


	import org.apache.log4j.Logger;

	public class Assi_1_Part_1 {
		
		final static Logger log = Logger.getLogger(Assi_1_Part_1.class);
		
		public static void main(String args[]) throws Exception{
			
			String dir = "/user/axp161730/assignment_1/part1";
			
			try {
				
				String[] booklist = { 	"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/20417.txt.bz2",
										"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/5000-8.txt.bz2",
										"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/132.txt.bz2",
										"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/1661-8.txt.bz2",
										"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/972.txt.bz2",
										"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/19699.txt.bz2"
									};
					
				String[] bookNames = {	"TheOutlineOfScience.txt.bz2",
										"TheNotebooksOfLeonardoDaVinci.txt.bz2",
										"ArtOfWar.txt.bz2",
										"TheAdventuresOfSherlockHolmes.txt.bz2",
										"TheDevilsDictionary.txt.bz2",
										"EncyclopediaBrit.txt.bz2"
									};
				//int i=0;
				for(int i=0;i<6;i++)
				{
					URL website = new URL(booklist[i]);
					
					FileOutputStream out = new FileOutputStream(bookNames[i]);
					ReadableByteChannel ch = Channels.newChannel(website.openStream());
					out.getChannel().transferFrom(ch, 0, Long.MAX_VALUE);
					out.close();
					
					log.info("File downloaded "+ bookNames[i]);
					String books[] = {"./"+bookNames[i],dir+bookNames[i]};
					FileCopyWithProgress_copy c = new FileCopyWithProgress_copy();
					c.copy(books);
					log.info("File copied to hdfs");
					FileDecompressor_copy decomp = new FileDecompressor_copy();
					decomp.decompress(dir+bookNames[i]);
					log.info("File Decompressed "+bookNames[i]);
					File file_to_delete = new File("./"+bookNames[i]);
					if(file_to_delete.delete()){
						log.info("File deleted "+file_to_delete.getName());
		    		}else{
		    			log.info("Delete failed");
		    		}
				}
				
			} catch (IOException e) {
				e.printStackTrace();
			}		
		}

	}
