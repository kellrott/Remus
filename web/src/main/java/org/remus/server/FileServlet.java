package org.remus.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class FileServlet extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 823762318603421923L;


	String name;
	File base;

	public FileServlet(String name, File base) {
		this.base = base;
		this.name = name;
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {

		String path = req.getPathInfo();
		if(path == null) {
			path = "index.html";
		}

		try {
			File f = new File(base,path);

			FileInputStream fis = new FileInputStream(f);
			byte [] buffer = new byte[1024];
			int len;

			OutputStream os = resp.getOutputStream();
			while ((len=fis.read(buffer))>0) {
				os.write(buffer, 0, len);
			}
		} catch (FileNotFoundException e) {
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
		}
	}

}
